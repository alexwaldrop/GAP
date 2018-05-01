import logging
import threading
import time

from System.Workers.Thread import Thread

class TaskWorker(Thread):

    INITIALIZING    = 0
    READY           = 1
    RUNNING         = 2
    COMPLETE        = 3
    CANCELLED       = 4

    def __init__(self, task, datastore, platform):
        # Class for executing task

        # Initialize new thread
        err_msg = "TaskWorker for %s has stopped working!" % task.get_ID()
        super(TaskWorker, self).__init__(err_msg)

        # Task to be executed
        self.task = task

        # Datastore for getting/setting task output
        self.datastore = datastore

        # Platform upon which task will be executed
        self.platform = platform

        # Status attributes
        self.status_lock = threading.Lock()
        self.status = TaskWorker.INITIALIZING

        # Processor for running task
        self.proc_id    = None
        self.cpus       = None
        self.mem        = None
        self.disk_space = None

    def set_status(self, new_status):
        # Updates instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            self.status = new_status

    def get_status(self):
        # Returns instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            return self.status

    def get_cpus(self):
        return self.cpus

    def get_mem(self):
        return self.mem

    def get_disk_space(self):
        return self.disk_space

    def task_to_run(self):

        # Set the input arguments
        self.__set_arguments()

        # Compute task resource requirements
        args            = self.task.get_input_args()
        self.nr_cpus    = args["nr_cpus"].get_value()
        self.mem        = args["mem"].get_value()
        self.disk_space = self.__compute_disk_requirements()

        # Set status to ready and wait to be launched
        self.set_status(TaskWorker.READY)

        while self.get_status() not in [TaskWorker.RUNNING, TaskWorker.CANCELLED]:
            # Wait for Scheduler to free up resources or cancel the job
            time.sleep(2)

        # Intialize processor, load files, run command once scheduler gives go ahead
        if TaskWorker.RUNNING:

            # Get the module command
            cmd = self.task.get_command(self.platform)

            # Run the module command if available
            if cmd is not None:

                job_name = self.task.get_ID()

                # Get a processor capable of running cmd
                proc_name = self.platform.get_processor(self.nr_cpus, self.mem, self.disk_space)

                # Load necessary input files
                self.platform.load_files(self.task.input_files)

                # Run command
                self.platform.run_command(proc_name, job_name, cmd, self.nr_cpus, self.mem, self.disk_space)

                # Save all output files
                self.platform.save_files(self.task.output_files)

                # Permanently save any final output files
                self.platform.persist_final_output(self.task.get_final_output())

                # Notify that task worker has completed
                self.set_status(TaskWorker.COMPLETE)

    def __set_resource_argument(self, arg_key, arg):

        # Search the argument key in the config input
        if arg_key in self.input_data["config_input"]:

            # Obtain the resource name
            res_name = self.input_data["config_input"][arg_key]

            # Get the resource object with name "res_name" from resource input data
            resource_obj = self.input_data["resource_input"][arg_key][res_name]

            # Set the argument with the resource path
            arg.set(resource_obj.get_path())

        # If not found in config input, search the argument key in the resource input
        elif arg_key in self.input_data["resource_input"]:

            # There should be only one resource of type "arg_key"
            resource_obj = self.input_data["resource_input"][arg_key].values()[0]

            # Set the argument with the resource path
            arg.set(resource_obj.get_path())

    def __set_argument(self, arg_key, arg):

        # Setup the input priority order
        input_order = ["module_input",
                       "node_input",
                       "sample_input",
                       "config_input"]

        # Search the key in each input type
        for input_type in input_order:
            if arg_key in self.input_data[input_type]:
                arg.set(self.input_data[input_type][arg_key])
                break

    def __set_arguments(self):

        # Get the arguments from the module
        arguments = self.module_obj.get_arguments()

        # Set the rest of the args
        for arg_key, arg in arguments.iteritems():

            # Set the argument depending if it is a resource
            if arg.is_resource():
                self.__set_resource_argument(arg_key, arg)
            else:
                self.__set_argument(arg_key, arg)

            # If not set yet, set with the default variable
            if not arg.is_set():
                arg.set(arg.get_default_value())

            # If still not set yet, raise an exception if the argument is required
            if not arg.is_set() and arg.is_mandatory():
                logging.error("In module %s, required input key \"%s\" could not be set." % (self.name, arg_key))
                raise IOError("Input could not be provided to the module %s " % self.name)

        # Make special changes to nr_cpus
        nr_cpus = arguments["nr_cpus"]
        if isinstance(nr_cpus.get_value(), basestring) and nr_cpus.get_value().lower() == "max":
            # Set special case for maximum nr_cpus
            nr_cpus.set(self.platform.get_max_nr_cpus())
        # Make sure nr cpus is an integer
        nr_cpus.set(int(nr_cpus.get_value()))

        # Make special changes to mem
        mem = arguments["mem"]
        if isinstance(mem.get_value(), basestring):
            if mem.get_value().lower() == "max":
                # Set special case where mem is max platform memory
                mem.set(self.platform.get_max_mem())
            elif "nr_cpus" in mem.get_value().lower():
                # Set special case if memory is scales with nr_cpus (e.g. 'nr_cpus * 1.5')
                nr_cpus     = nr_cpus.get_value()
                mem_expr    = mem.get_default_value().lower()
                mem_val     = int(eval(mem_expr.replace("nr_cpus", str(nr_cpus))))
                if mem_val < self.platform.get_max_mem():
                    mem.set(mem_val)
                else:
                    # Set to max memory if amount of memory requested exceeds limit
                    mem.set(self.platform.get_max_mem())
        # Make sure mem is an integer
        mem.set(int(mem.get_value()))

    def __compute_disk_requirements(self):
        pass