import logging
import importlib
import copy

class Node(object):

    def __init__(self, node_id, **kwargs):

        self.__node_id                = node_id

        # Get the module names
        self.__main_module_name       = kwargs.pop("module")

        # Identify if the framework runs in split mode
        self.__split_mode             = kwargs.pop("split")

        # Get the final output keys
        self.__final_output_keys      = kwargs.pop("final_output")

        # Get the config inputs
        self.__config_input           = kwargs.pop("args", [])

        # Initialize modules
        self.main_module    = self.__load_module(self.__main_module_name)

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
        module_id = "%s_%s" % (self.__node_id, module_name)

        return _class(module_id)

    def is_split_mode(self):
        return self.__split_mode

    def get_ID(self):
        return self.__node_id

    def get_config_input(self):
        return self.__config_input

    def get_output_keys(self):
        # Return output keys. get_keys() returns input_keys and output_keys.
        return self.main_module.get_keys()[1]

    def get_final_output_keys(self):
        return self.__final_output_keys

    def split(self, split_id):
        split_node = copy.deepcopy(self)
        split_node.__node_id = split_id
        return split_node
