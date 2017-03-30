import importlib
import logging
import threading
import Queue
import sys

def initialize_module(module_name):

    d = dict()
    d["module_name"] = module_name
    d["module"] = importlib.import_module("GAP_modules.%s" % d["module_name"])
    d["class_name"] = d["module"].__main_class__
    d["class"] = d["module"].__dict__[d["class_name"]]

    return d

class Node(threading.Thread):


    class SplitServer(threading.Thread):
        """ Exception throwing method taken from:
                http://stackoverflow.com/questions/2829329/catch-a-threads-exception-in-the-caller-thread-in-python
        """

        def __init__(self, server_name, platform, job_name, cmd, **kwargs):
            super(Node.SplitServer, self).__init__()

            self.daemon = True
            self.exception_queue = Queue.Queue()

            self.platform = platform
            self.server_name = server_name
            self.server_obj = None

            self.job_name = job_name
            self.cmd = cmd

            self.kwargs = kwargs

        def run_with_exception(self):

            # Creating split server
            self.platform.create_split_server(self.server_name, **self.kwargs)
            self.server_obj = self.platform.instances[self.server_name]

            # Waiting for split server to be created
            self.server_obj.wait_process("create")

            # Running the command on instance
            self.server_obj.run_command(self.job_name, self.cmd)

            # Waiting for the job to finish
            self.server_obj.wait_process(self.job_name)

            # If no exceptions were raised and we reach this point
            self.server_obj.destroy()

            # Waiting for split server to be destroyed
            self.server_obj.wait_process("destroy")

        def run(self):
            try:
                self.run_with_exception()
            except BaseException as e:
                if e.message != "":
                    logging.error("(%s) Exception in executing thread: %s." % (self.server_name, e.message))
                self.exception_queue.put(sys.exc_info())
            else:
                self.exception_queue.put(None)

        def wait(self):

            # Check if thread is not running
            if not self.is_alive():

                # Check if thread has already finished (queue should not be empty)
                if self.exception_queue.empty():
                    return

            # If running, or finished already, wait for possible exception to appear
            exc_info = self.exception_queue.get()
            if exc_info is not None:
                raise exc_info[0], exc_info[1], exc_info[2]


    def __init__(self, config, platform, sample_data, module_name):
        super(Node, self).__init__()

        self.daemon = True
        self.exception_queue = Queue.Queue()

        self.config = config
        self.platform = platform
        self.sample_data = sample_data
        self.module_name = module_name

        # Importing main module
        try:
            self.main = initialize_module(module_name)
            self.main_obj = self.main["class"](self.config, self.sample_data)
        except ImportError:
            logging.error("Module %s cannot be imported!" % module_name)
            exit(1)

        # Importing splitter and merger:
        if self.main_obj.can_split:

            try:
                self.split = initialize_module(self.main_obj.splitter)
                self.split_obj = self.split["class"](self.config, self.sample_data)
            except ImportError:
                logging.error("Module %s cannot be imported!" % self.main_obj.splitter)
                exit(1)

            try:
                self.merge = initialize_module(self.main_obj.merger)
                self.merge_obj = self.merge["class"](self.config, self.sample_data)
            except ImportError:
                logging.error("Module %s cannot be imported!" % self.main_obj.merger)
                exit(1)

        self.split_outputs = None
        self.main_outputs  = None
        self.merge_outputs = None

        self.complete      = False

    def run_split(self):

        # Creating job names
        split_job_name  = "%s_split" % self.module_name
        main_job_name   = lambda splt_id: "%s_%d" % (self.module_name, splt_id)
        merge_job_name  = "%s_merge" % self.module_name

        # Running the splitter
        cmd = self.split_obj.get_command( nr_splits=self.config["general"]["nr_splits"] )
        if cmd is not None:
            self.platform.instances["main-server"].run_command(split_job_name, cmd)
            self.platform.instances["main-server"].wait_process(split_job_name)

        self.split_outputs = self.split_obj.get_output()
        self.set_pipeline_output(self.split_obj.get_pipeline_output())

        self.main_outputs = list()

        # Creating the split server threads
        split_servers = dict()
        for split_id, args in enumerate(self.split_outputs):

            # Generating split server name
            server_name = "%s-split%d-server" % (self.module_name.lower(), split_id)

            # Obtaining main command
            cmd = self.main_obj.get_command(split_id=split_id, **args)

            # Obtaining outputs
            self.main_outputs.append(self.main_obj.get_output())
            self.set_pipeline_output(self.main_obj.get_pipeline_output())

            # Checking if there is command to run
            if cmd is not None:
                # Creating SplitServer object
                split_servers[server_name] = self.SplitServer(server_name, self.platform, main_job_name(split_id), cmd)

                # Starting split server work
                split_servers[server_name].start()

        # Waiting for all the split processes to finish
        for server_thread in split_servers.itervalues():
            server_thread.wait()

        # Running the merger
        cmd = self.merge_obj.get_command( nr_splits=self.config["general"]["nr_splits"],
                                          inputs=self.main_outputs )
        self.platform.instances["main-server"].run_command(merge_job_name, cmd)
        self.platform.instances["main-server"].wait_process(merge_job_name)

        self.set_pipeline_output(self.merge_obj.get_pipeline_output())

    def run_normal(self):

        cmd = self.main_obj.get_command()

        self.set_pipeline_output(self.main_obj.get_pipeline_output())

        if cmd is None:
            logging.info("Module %s has generated no command." % self.module_name)
            return None

        self.platform.instances["main-server"].run_command(self.module_name, cmd)
        self.platform.instances["main-server"].wait_process(self.module_name)

    def run(self):

        try:

            if self.main_obj.can_split and self.config["general"]["split"]:
                self.run_split()
            else:
                self.run_normal()

        except BaseException as e:
            if e.message != "":
                logging.error("Exception in executing node with module '%s': %s." % (self.module_name, e.message))
            self.exception_queue.put(sys.exc_info())
        else:
            self.exception_queue.put(None)
        finally:
            self.complete = True

    def set_pipeline_output(self, output):

        def flatten(lst):
            if isinstance(lst, list):
                return [x for el in lst for x in flatten(el)]
            else:
                return [lst]

        # Checking if there is output to set
        if output is None:
            return

        # Ensuring the "outputs" key is present
        if "outputs" not in self.sample_data:
            self.sample_data["outputs"] = dict()

        # Setting the output as pipeline output
        if self.module_name in self.sample_data["outputs"]:
            self.sample_data["outputs"][self.module_name].extend(flatten(output))
        else:
            self.sample_data["outputs"][self.module_name] = flatten(output)

    def finalize(self):

        # Check if the list is empty
        if self.exception_queue.empty():
            return

        # Raise an exception if required
        exc_info = self.exception_queue.get()
        if exc_info is not None:
            raise exc_info[0], exc_info[1], exc_info[2]