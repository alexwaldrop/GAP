import logging

from Thread import Thread

class ModuleWorker(Thread):

    def __init__(self, platform, module_obj, job_name, split_name=None):

        # Get the name of the module that is processed
        self.module_id = module_obj.get_ID()

        # Initialize the thread
        err_msg = "ModuleWorker for %s has stopped working!" % self.module_id
        super(ModuleWorker, self).__init__(err_msg)

        # Get the necessary variables
        self.platform   = platform
        self.module_obj = module_obj
        self.job_name   = job_name
        self.split_name = split_name

        # Initialize the input data
        self.input_data     = {}

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
            nr_cpus.set(self.platform.get_max_nr_cpus)
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

    def task(self):

        # Set the input arguments
        self.__set_arguments()

        # Obtain the value of arguments "nr_cpus" and "mem"
        args    = self.module_obj.get_arguments()
        nr_cpus = args["nr_cpus"].get_value()
        mem     = args["mem"].get_value()

        # Get the module command
        cmd = self.module_obj.get_command(self.platform, split_name=self.split_name)

        # Run the module command if available
        if cmd is not None:
            if self.module_obj.is_quick_command():
                # Run command on main processor if module generates a quick command
                self.platform.run_quick_command(self.job_name, cmd)
            else:
                # Otherwise spawn new processor on platform with resources necessary to run module command
                self.platform.run_command(self.job_name, cmd, nr_cpus, mem)

    def set_input(self, **kwargs):

        # Get the input values
        self.input_data["module_input"]     = kwargs.get("module_input",    [])
        self.input_data["node_input"]       = kwargs.get("node_input",      [])
        self.input_data["sample_input"]     = kwargs.get("sample_input",    [])
        self.input_data["config_input"]     = kwargs.get("config_input",    [])
        self.input_data["resource_input"]   = kwargs.get("resource_input",  [])

    def get_output(self):
        return self.module_obj.get_output()