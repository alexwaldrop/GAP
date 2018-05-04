import logging
import importlib
import copy

class Task(object):

    def __init__(self, task_id, **kwargs):

        self.__task_id              = task_id

        # Get the module names
        self.__module_name          = kwargs.pop("module")

        # Get the final output keys
        self.__final_output_keys    = kwargs.pop("final_output")

        # Get the config inputs
        self.__module_args          = kwargs.pop("args", [])

        # Initialize modules
        self.module                 = self.__load_module(self.__module_name)

        # Whether task has been completed
        self.complete   = False

        # Resources required to run task
        self.cpus       = None
        self.mem        = None
        self.disk_space = None

        # Boolean for whether task was created by splitting an existing task
        self.__is_split = False

        # Upstream splitter task that created split task
        self.__splitter = None

        # Output partition visible to task
        self.__split_id = None

        # Sample partition visible to task
        self.__visible_samples = "All"

    #@property
    #def module_type(self):
    #    # Determine whether process is splitter, merger, or standard tool
    #    if isinstance(self.module, "splitter"):
    #        return "Splitter"
    #    elif isinstance(self.module, "merger"):
    #        return "Merger"
    #    else:
    #        return "Tool"

    def set_complete(self, is_complete):
        self.complete = is_complete

    def split(self, new_id, splitter, split_id, visible_samples):
        # Produce clone of current task but restrict visible output and sample info available to task
        # Visible output/sample partition defined by upstream splitting task
        # Splitter is the name of the head task that created the split of current task
        # Split_id is the name of the partition the newly created task will be able to access
        # visible_samples is list of samples visible to new split

        # Create copy of current task and give new id
        split_task = copy.deepcopy(self)
        split_task.__task_id = new_id

        # Upstream task responsible for creating new split task
        split_task.__splitter = splitter

        # Split visible to this split task
        split_task.__split_id = split_id

        # Samples visible to split task
        split_task.__visible_samples = visible_samples

        # Specify that new split task is the result of a split
        split_task.__is_split = True

        return split_task

    def get_ID(self):
        return self.__task_id

    def get_module(self):
        return self.module

    def get_input_args(self):
        return self.module.get_arguments()

    def get_output(self):
        return self.module.get_output()

    def get_input_keys(self):
        # Return input keys. get_keys() returns input_keys and output_keys.
        return self.module.get_keys()[0]

    def get_output_keys(self):
        # Return output keys. get_keys() returns input_keys and output_keys.
        return self.module.get_keys()[1]

    def get_final_output_keys(self):
        return self.__final_output_keys

    def get_graph_config_args(self):
        return self.__module_args

    def get_command(self, platform):
        return self.module.get_command(platform, split_id=self.__split_id)

    def get_cpus(self):
        return self.cpus

    def get_mem(self):
        return self.mem

    def get_disk_space(self):
        return self.disk_space

    def set_cpus(self, cpus):
        self.cpus = cpus

    def set_mem(self, mem):
        self.mem = mem

    def set_disk_space(self, disk_space):
        self.disk_space = disk_space

    def is_complete(self):
        return(self.complete)

    def is_split(self):
        return self.__is_split

    def get_visible_samples(self):
        return self.__visible_samples

    def get_splitter(self):
        return self.__splitter

    def get_split_id(self):
        return self.__split_id

    def __load_module(self, module_name):

        # Try importing the module
        try:
            _module = importlib.import_module(module_name)
        except:
            logging.error("Module %s could not be imported! "
                          "Check the module name spelling and ensure the module exists." % module_name)
            raise

        # Get the class
        _class = _module.__dict__[module_name]

        # Generate the module ID
        module_id = "%s_%s" % (self.__task_id, module_name)

        return _class(module_id)
