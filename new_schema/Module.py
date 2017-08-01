import abc
import logging

from Argument import Argument

class Module(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, node_id):

        # Initialize the module ID, defined in the config
        self.node_id = node_id

        # Initialize Current module name
        self.name = self.__class__.__name__

        # Initialize the input arguments
        self.arguments = None

        # Initialize the output variables
        self.output = None

        # Initialize the lists of the input and output keys
        self.input_keys = None
        self.output_keys = None

    def add_argument(self, key, is_required=False, is_resource=False, default_value=None):

        if key in self.arguments:
            logging.error("In module %s, the input argument '%s' is defined multiple time!" % (self.name, key))
            raise RuntimeError("Input argument '%s' has been defined multiple times!" % key)

        self.arguments[key] = Argument( key,
                                        is_required=is_required,
                                        is_resource=is_resource,
                                        default_value=default_value)

    def add_output(self, key, value):

        if key in self.output:
            logging.error("In module %s, the output key '%s' is defined multiple time!" % (self.name, key))
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

    def get_keys(self):
        return self.input_keys, self.output_keys

    def get_arguments(self):
        return self.arguments

    def get_output(self):
        return self.output

    def get_command(self, platform, **kwargs):

        # Define the output
        self.define_output(platform, **kwargs)

        # Define the command
        cmd = self.define_command(platform, **kwargs)

        return cmd

    def generate_path(self, platform, prefix=None, split_name=None, extension=".dat", is_dir=False):

        # Get the workspace directory
        workspace_dir = platform.get_workspace_dir()

        # Initialize the path variable with the workspace dir
        path = workspace_dir

        # Append prefix
        if prefix is not None:
            path += prefix
        else:
            path += "%s_%s" % (self.name, self.node_id)

        # Append split name if present
        if split_name is not None:
            path += "_%s" % split_name

        # Append extension if necessary
        if is_dir:
            path += "/"
        else:
            path += extension

        return path
