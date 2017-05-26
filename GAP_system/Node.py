import importlib
import logging

from GAP_system import Thread

class Node(Thread):

    class SplitServer(Thread):

        def __init__(self, server_name, platform, job_name, cmd, **kwargs):
            super(Node.SplitServer, self).__init__()

            self.platform = platform
            self.server_name = server_name
            self.server_obj = None

            self.job_name = job_name
            self.cmd = cmd

            self.kwargs = kwargs

            # Setting up the error message in case the thread raises an exception
            self.set_error_message("(%s) Exception in executing thread" % self.server_name)

        def task(self):

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

    def __init__(self, config, platform, sample_data, **kwargs):
        super(Node, self).__init__()

        # Setting base variables
        self.config         = config
        self.platform       = platform
        self.sample_data    = sample_data
        self.kwargs         = kwargs

        # Obtaining configuration
        self.tool_id            = kwargs.get("tool_id")
        self.module_name        = kwargs.get("module")
        self.final_output_keys  = kwargs.get("final_output",        list())

        # Input data for the entire node
        self.input_data = None

        # Output data from all the sections of the node
        self.split_outputs = None
        self.main_outputs  = None
        self.merge_outputs = None

        # Setting up the error message in case the thread raises an exception
        self.set_error_message("Exception in executing node with module '%s'" % self.module_name)

        # Importing main module
        try:
            self.main = Node.init_module(self.module_name, is_tool=True)
            self.main_obj = self.main["class"](self.config, self.sample_data, self.tool_id)
        except ImportError:
            logging.error("Module %s cannot be imported!" % self.module_name)
            exit(1)

        # Identify if the node is running in split mode
        self.is_split_mode = self.main_obj.can_split and self.config["general"]["split"]

        # Importing splitter and merger:
        if self.is_split_mode:

            try:
                self.split = Node.init_module(self.main_obj.splitter, is_splitter=True)
                self.split_obj = self.split["class"](self.config, self.sample_data, self.tool_id, main_module_name=self.module_name)
            except ImportError:
                logging.error("Module %s cannot be imported!" % self.main_obj.splitter)
                exit(1)

            try:
                self.merge = Node.init_module(self.main_obj.merger, is_merger=True)
                self.merge_obj = self.merge["class"](self.config, self.sample_data, self.tool_id, main_module_name=self.module_name)
            except ImportError:
                logging.error("Module %s cannot be imported!" % self.main_obj.merger)
                exit(1)

    @staticmethod
    def init_module(module_name, is_tool=False, is_splitter=False, is_merger=False):

        d = dict()
        d["module_name"] = module_name

        if is_tool:
            d["module"] = importlib.import_module("GAP_modules.Tools.%s" % d["module_name"])
        elif is_splitter:
            d["module"] = importlib.import_module("GAP_modules.Splitters.%s" % d["module_name"])
        elif is_merger:
            d["module"] = importlib.import_module("GAP_modules.Mergers.%s" % d["module_name"])
        else:
            logging.error("Module %s could not be imported! Specify whether module is tool, splitter, or merger!")

        d["class_name"] = d["module"].__main_class__
        d["class"] = d["module"].__dict__[d["class_name"]]

        return d

    def run_split(self):

        # Creating job names
        split_job_name  = "%s_split" % self.module_name
        main_job_name   = lambda splt_id: "%s_%d" % (self.module_name, splt_id)
        merge_job_name  = "%s_merge" % self.module_name

        # Running the splitter
        cmd = self.split_obj.generate_command(**self.input_data)

        if cmd is not None:
            self.platform.instances["main-server"].run_command(split_job_name, cmd)
            self.platform.instances["main-server"].wait_process(split_job_name)

        # Obtaining the splitter outputs
        self.split_outputs = self.split_obj.get_output()

        self.main_outputs = list()

        # Creating the split server threads
        split_servers = dict()
        for split_id, args in enumerate(self.split_outputs):

            # Obtaining main command
            cmd = self.main_obj.generate_command(split_id=split_id, **args)

            # Obtaining the main output
            self.main_outputs.append(self.main_obj.get_output())

            # Checking if there is command to run
            if cmd is not None:

                # Generating split server name
                server_name = "%s-split%d-server" % (self.module_name.lower(), split_id)

                # Obtaining required resources
                kwargs = dict()
                kwargs["nr_cpus"] = self.main_obj.get_nr_cpus()
                kwargs["mem"] = self.main_obj.get_mem()

                # Creating SplitServer object
                split_servers[server_name] = self.SplitServer(server_name, self.platform, main_job_name(split_id), cmd, **kwargs)

                # Starting split server work
                split_servers[server_name].start()

        # Waiting for all the split processes to finish
        done = False
        while not done:

            # Assume all server_threads have finished
            done = True

            # Check each server if it has finished
            for server_thread in split_servers.itervalues():
                if server_thread.is_done():
                    server_thread.finalize()
                else:
                    done = False

        # Convert the input to a dictionary of lists
        merge_input = dict()
        for key in self.main_outputs[0]:
            merge_input[key] = [ main_output[key] for main_output in self.main_outputs]

        # Running the merger
        cmd = self.merge_obj.generate_command(**merge_input)
        self.platform.instances["main-server"].run_command(merge_job_name, cmd)
        self.platform.instances["main-server"].wait_process(merge_job_name)

        # Obtaining the merger outputs
        self.merge_outputs = self.merge_obj.get_output()

    def run_normal(self):

        # Obtaining main command
        cmd = self.main_obj.generate_command(**self.input_data)

        # Obtaining output that will be saved at the end of the pipeline
        self.main_outputs = self.main_obj.get_output()

        if cmd is None:
            logging.debug("Module %s has generated no command." % self.module_name)
            return None

        self.platform.instances["main-server"].run_command(self.module_name, cmd)
        self.platform.instances["main-server"].wait_process(self.module_name)

    def task(self):

        if self.is_split_mode:
            self.run_split()
        else:
            self.run_normal()

        self.set_final_output()

    def get_module_name(self):
        return self.module_name

    def check_input(self, input_keys):

        if self.is_split_mode:

            # Checking the splitter
            not_found_keys      = self.split_obj.check_input(input_keys)
            if not_found_keys:
                return "The splitter expected the following input keys: %s!" % " ".join(not_found_keys)

            # Checking the main object
            split_output_keys   = self.split_obj.define_output()
            not_found_keys      = self.main_obj.check_input(split_output_keys, splitted=True)
            if not_found_keys:
                return "The following input keys were expected from the splitter: %s!" % " ".join(not_found_keys)

            # Checking the merger
            main_output_keys    = self.main_obj.define_output(splitted=True)
            not_found_keys      = self.merge_obj.check_input(main_output_keys)
            if not_found_keys:
                return "The merger expected the following input keys: %s!" % " ".join(not_found_keys)

        else:
            not_found_keys      = self.main_obj.check_input(input_keys)
            if not_found_keys:
                return "The following input keys were expected: %s" % " ".join(not_found_keys)

        # Input is correct
        return None

    def check_output(self, output_keys):

        # If empty set of output keys are required, no need to check output anymore
        if len(output_keys) == 0:
            return None

        output_keys_list = list()

        if self.is_split_mode:

            output_keys_list.extend(self.split_obj.define_output())
            output_keys_list.extend(self.main_obj.define_output())
            output_keys_list.extend(self.merge_obj.define_output())

        else:

            output_keys_list.extend(self.main_obj.define_output())

        not_found = list()
        for key in output_keys:
            if key not in output_keys_list:
                not_found.append(key)

        if not_found:
            return "The following keys could not be found in the output: %s" % " ".join(not_found)
        else:
            return None

    def check_requirements(self):

        # Generating an aggregated not_found dictionary
        not_found = dict()
        not_found["tools"] = list()
        not_found["resources"] = list()

        # Generating the list of modules to check
        if self.is_split_mode:
            modules_to_check = [self.split_obj, self.main_obj, self.merge_obj]
        else:
            modules_to_check = [self.main_obj]

        # Obtiaining the list of requirements that are not met
        for module_obj in modules_to_check:
            not_found_in_module = module_obj.check_requirements()
            not_found["tools"].extend( not_found_in_module["tools"] )
            not_found["resources"].extend( not_found_in_module["resources"] )

        return not_found

    def define_output(self):
        if self.is_split_mode:
            return self.merge_obj.define_output()
        else:
            return self.main_obj.define_output()

    def set_input(self, input_list):

        # Convert from list of dictionary to dictionary of lists
        input_data = dict()
        for input_dict in input_list:
            for key in input_dict:

                # Initialize the input_data
                if key not in input_data:
                    input_data[key] = list()

                # Add value to the input data
                input_data[key].append( input_dict[key] )

        # Each list of one element, will be converted to the element
        self.input_data = dict()
        for key in input_data:
            if len(input_data[key]) == 1:
                self.input_data[key] = input_data[key][0]
            else:
                self.input_data[key] = input_data[key]

        logging.debug("Module %s has received the following input: %s" % (self.module_name, self.input_data))

    def get_output(self):
        if self.is_split_mode:
            return self.merge_outputs
        else:
            return self.main_outputs

    def define_final_output(self):
        return self.final_output_keys

    def set_final_output(self):

        # Initialize the final output in the sample_data
        if "final_output" not in self.sample_data:
            self.sample_data["final_output"] = dict()

        # Check if the module is already in the final_output
        if self.module_name not in self.sample_data["final_output"]:
            self.sample_data["final_output"][self.module_name] = list()

        # Get every key that is first found in the merger output or the splits output
        for key in self.final_output_keys:

            # Add all the paths to the final output depending on the running method
            if self.is_split_mode:

                # Search the key in the merger output
                if key in self.merge_outputs:
                    self.sample_data["final_output"][self.module_name].append( self.merge_outputs[key] )

                # Search the key in the main object output
                elif key in self.main_outputs[0]:
                    for main_output in self.main_outputs:
                        self.sample_data["final_output"][self.module_name].append( main_output[key] )

                # Search the key in the splitter output
                elif key in self.split_outputs[0]:
                    for split_output in self.split_outputs:
                        self.sample_data["final_output"][self.module_name].append( split_output[key] )

            else:

                # Search the key in the main object output
                if key in self.main_outputs:
                    self.sample_data["final_output"][self.module_name].append( self.main_outputs[key] )
