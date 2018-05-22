import abc
import logging
import os

from System.Datastore import GAPFile

class Module(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, module_id):

        # Initialize the module ID, defined in the config
        self.module_id = module_id

        # Initialize the input arguments
        self.arguments = {}
        self.define_input()

        # Initialize the output variables
        self.output = {}

        # Initialize the lists of the input and output keys
        self.input_keys = self.arguments.keys()
        self.output_keys = None

        # Module output file directory
        self.output_dir = "/tmp/"

    @abc.abstractmethod
    def define_input(self):
        pass

    @abc.abstractmethod
    def define_output(self):
        pass

    @abc.abstractmethod
    def define_command(self):
        pass

    def add_argument(self, key, is_required=False, is_resource=False, default_value=None):

        if key in self.arguments:
            logging.error("In module %s, the input argument '%s' is defined multiple time!" % (self.module_id, key))
            raise RuntimeError("Input argument '%s' has been defined multiple times!" % key)

        self.arguments[key] = Argument(key,
                                       is_required=is_required,
                                       is_resource=is_resource,
                                       default_value=default_value)

    def add_output(self, key, value, is_path=True, **kwargs):
        if key in self.output:
            logging.error("In module %s, the output key '%s' is defined multiple time!" % (self.module_id, key))
            raise RuntimeError("Output key '%s' has been defined multiple times!" % key)

        if is_path and not isinstance(value, GAPFile):
            # Convert paths to GAPFiles if they haven't already been converted
            file_id = "%s.%s" % (self.module_id, key)
            self.output[key] = GAPFile(file_id, file_type=key, path=value, **kwargs)

        else:
            self.output[key] = value

    def get_command(self):

        # Check that all required inputs are set
        err = False
        for arg_type, arg in self.arguments.iteritems():

            # Set argument to default value if it hasn't been set
            if not arg.is_set():
                arg.set(arg.get_default_value())

            # Throw error if mandatory argument hasn't been set at runtime
            if not arg.is_set() and arg.is_mandatory():
                logging.error("Module of type %s with id %s missing required input type: %s" % (self.__class__.__name__, self.module_id, arg_type))
                err = True
        if err:
            # Raise error if any required arguments have not been set
            raise RuntimeError("Module could not generate command! Required inputs missing at runtime!")

        # Define the names of output files given an output directory (default: self.output_dir)
        self.define_output()

        # Define the command
        return self.define_command()

    def process_cmd_output(self, out, err):
        # Function to be overriden by inheriting classes that process output from their command to set one of their outputs
        # Example: Module that determines how many lines are in a file
        pass

    def generate_unique_file_name(self, extension=".dat", output_dir=None):

        # Generate file basename
        path = self.module_id

        # Standardize and append extension
        extension = ".%s" % extension.lstrip(".")
        path += extension

        # Join with output dir other than self.output_dir
        if output_dir is not None:
            path = os.path.join(output_dir, path)
        # Otherwise default to creating output files in self.output_dir
        else:
            path = os.path.join(self.output_dir, path)

        return path

    ############### Getters and setters
    def get_ID(self):
        return self.module_id

    def set_ID(self, new_id):
        self.module_id = new_id

    def get_input_types(self):
        return self.input_keys

    def get_output_types(self):
        return self.output_keys

    def set_argument(self, key, value):
        # Set value for input argument
        if key not in self.arguments:
            logging.error("Attempt to set undeclared input '%s' for module with id '%s' of type %s!" % (key,
                                                                                                        self.module_id,
                                                                                                        self.__class__.__name__))
            raise RuntimeError("Attempt to set undeclared input type for module!")
        self.arguments[key].set(value)

    def get_argument(self, key):
        # Return value of an input argument
        if key not in self.arguments:
            logging.error("Attempt to get undeclared input '%s' for module with id '%s' of type %s!" % (key,
                                                                                                        self.module_id,
                                                                                                        self.__class__.__name__))
            raise RuntimeError("Attempt to get undeclared input type for module!")
        return self.arguments[key].get_value()

    def get_arguments(self):
        return self.arguments

    def get_output(self, key=None):
        if key is None:
            return self.output
        return self.output[key]

    def set_output(self, key, value):
        # Set value for output object
        if key not in self.output_keys:
            logging.error("Attempt to set undeclared output type '%s' for module with id '%s' of type %s" % (key,
                                                                                                             self.module_id,
                                                                                                             self.__class__.__name__))
            raise RuntimeError("Attempt to set undeclared output type for module!")
        self.output[key] = value

    def get_output_dir(self):
        return self.output_dir

    def set_output_dir(self, new_output_dir):
        self.output_dir = new_output_dir


class Argument(object):
    # Class for holding data and metadata for module input arguments
    def __init__(self, name, is_required=False, is_resource=False, default_value=None):

        self.__name = name

        self.__is_required = is_required
        self.__is_resource = is_resource

        self.__default_value = default_value

        self.__value = None

    def set(self, value):
        self.__value = value

    def get_name(self):
        return self.__name

    def get_default_value(self):
        return self.__default_value

    def get_value(self):
        return self.__value

    def is_set(self):
        return self.__value is not None

    def is_mandatory(self):
        return self.__is_required

    def is_resource(self):
        return self.__is_resource
