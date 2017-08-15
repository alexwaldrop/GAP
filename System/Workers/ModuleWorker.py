import logging

from Thread import Thread

class ModuleWorker(Thread):

    def __init__(self, platform, module_obj, job_name):

        # Get the name of the module that is processed
        self.module_id = module_obj.get_ID()

        # Initialize the thread
        err_msg = "ModuleWorker for %s has stopped working!" % self.module_id
        super(ModuleWorker, self).__init__(err_msg)

        # Get the necessary variables
        self.platform   = platform
        self.module_obj = module_obj
        self.job_name   = job_name

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

        # Set nr_cpus
        nr_cpus_arg = arguments["nr_cpus"]
        self.__set_argument("nr_cpus", nr_cpus_arg)
        if not nr_cpus_arg.is_set():
            # Set special case for maximum nr_cpus
            if isinstance(nr_cpus_arg.get_default_value(), basestring):
                if nr_cpus_arg.get_default_value().lower() == "max":
                    nr_cpus_arg.set(self.platform.get_max_nr_cpus())
            else:
                nr_cpus_arg.set(nr_cpus_arg.get_default_value())

        else:
            # Make sure nr cpus is an integer
            nr_cpus_arg.set(int(nr_cpus_arg.get_value()))

        # Set mem
        mem_arg = arguments["mem"]
        self.__set_argument("mem", mem_arg)
        if not mem_arg.is_set():
            if isinstance(mem_arg.get_default_value(), basestring):
                if mem_arg.get_default_value().lower() == "max":
                    # Set special case for maximum mem
                    mem_arg.set(self.platform.get_max_mem())
                elif "nr_cpus" in mem_arg.get_default_value().lower():
                    # Set special case if memory is scales with nr_cpus (e.g. 'nr_cpus * 1.5')
                    nr_cpus     = nr_cpus_arg.get_value()
                    mem_expr    = mem_arg.get_default_value().lower()
                    mem         = int(eval(mem_expr.replace("nr_cpus", str(nr_cpus))))
                    if mem < self.platform.get_max_mem():
                        mem_arg.set(mem)
                    else:
                    # Set to max memory if amount of memory requested exceeds limit
                        mem_arg.set(self.platform.get_max_mem())
            # Set default value
            else:
                mem_arg.set(mem_arg.get_default_value())
        else:
            # Make sure mem is an integer
            mem_arg.set(int(mem_arg.get_value()))

        # Set the rest of the args
        for arg_key, arg in arguments.iteritems():

            if arg_key not in ["nr_cpus", "mem"]:

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

    def task(self):

        # Set the input arguments
        self.__set_arguments()

        # Obtain the value of arguments "nr_cpus" and "mem"
        args    = self.module_obj.get_arguments()
        nr_cpus = args["nr_cpus"].get_value()
        mem     = args["mem"].get_value()

        # Get the module command
        cmd = self.module_obj.get_command(self.platform, split_name=self.input_data["split_name"])

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
        self.input_data["split_name"]       = kwargs.get("split_name",      None)

    def get_output(self):
        return self.module_obj.get_output()