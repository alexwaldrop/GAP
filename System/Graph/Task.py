import logging
import importlib
import copy

class Task(object):

    def __init__(self, task_id, **kwargs):

        self.__task_id              = task_id

        # Split id
        self.__split_id             = None

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

    def split(self, split_id):
        split_node = copy.deepcopy(self)
        split_node.__task_id = split_id
        return split_node

    def get_ID(self):
        return self.__task_id

    def get_module(self):
        return self.module

    def get_split_id(self):
        return self.__split_id

    def get_input_keys(self):
        # Return input keys. get_keys() returns input_keys and output_keys.
        return self.module.get_keys()[0]

    def get_output_keys(self):
        # Return output keys. get_keys() returns input_keys and output_keys.
        return self.module.get_keys()[1]

    def get_final_output_keys(self):
        return self.__final_output_keys

    def get_user_module_args(self):
        return self.__module_args

    def get_command(self, platform):
        return self.module.get_command(platform, split_id=self.__split_id)

    def get_output(self):
        return self.module.get_output()

    def get_output_files(self):
        return self.module.get_output_files()

    def is_complete(self):
        return(self.complete)

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
