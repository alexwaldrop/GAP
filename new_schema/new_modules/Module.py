import abc
import logging
import os

from Argument import Argument

class Module(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, module_id):

        # Initialize the module ID, defined in the config
        self.module_id = module_id

        # Initialize the input arguments
        self.arguments = None

        # Initialize the output variables
        self.output = None

        # Initialize the lists of the input and output keys
        self.input_keys = None
        self.output_keys = None

    def add_argument(self, key, is_required=False, is_resource=False, default_value=None):

        if key in self.arguments:
            logging.error("In module %s, the input argument '%s' is defined multiple time!" % (self.module_id, key))
            raise RuntimeError("Input argument '%s' has been defined multiple times!" % key)

        self.arguments[key] = Argument( key,
                                        is_required=is_required,
                                        is_resource=is_resource,
                                        default_value=default_value)

    def add_output(self, key, value):

        if key in self.output:
            logging.error("In module %s, the output key '%s' is defined multiple time!" % (self.module_id, key))
            raise RuntimeError("Output key '%s' has been defined multiple times!" % key)

        self.output[key] = value

    @abc.abstractmethod
    def define_input(self):
        pass

    @abc.abstractmethod
    def define_output(self, platform, **kwargs):
        pass

    @abc.abstractmethod
    def define_command(self, platform, **kwargs):
        pass

    def get_ID(self):
        return self.module_id

    def get_keys(self):
        return self.input_keys, self.output_keys

    def get_arguments(self, key=None):
        if key is None:
            return self.arguments
        return self.arguments[key]

    def get_output(self, key=None):
        if key is None:
            return self.output
        return self.output[key]

    def get_command(self, platform, **kwargs):

        # Define the output
        self.define_output(platform, **kwargs)

        # Define the command
        cmd = self.define_command(platform, **kwargs)

        return cmd

    def generate_file_name(self, file_prefix, split_name=None, extension=".dat"):

        path = file_prefix

        if file_prefix is not None:
            path = os.path.join(path, file_prefix)
        else:
            path = os.path.join(path, self.module_id)

        # Append split name if present
        if split_name is not None:
            path += "_%s" % split_name

        # Append extension
        path += extension
        return path
