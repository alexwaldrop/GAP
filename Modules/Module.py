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
        self.required = []
        self.define_input()

        # Initialize the output variables
        self.output = {}

        # Initialize the lists of the input and output keys
        self.input_keys = self.arguments.keys()
        self.output_keys = None

        # Module output file directory
        self.output_dir = "/tmp/"

        # Specify whether module is tool or merger
        self.merger = False

    @abc.abstractmethod
    def define_input(self):
        pass

    @abc.abstractmethod
    def define_output(self):
        pass

    @abc.abstractmethod
    def define_command(self):
        pass

    def add_argument(self, key, is_required=False, default_value=None):

        if key in self.arguments:
            logging.error("In module %s, the input argument '%s' is defined multiple time!" % (self.module_id, key))
            raise RuntimeError("Input argument '%s' has been defined multiple times!" % key)

        self.arguments[key] = default_value
        if is_required:
            self.required.append(key)

    def add_output(self, key, value, is_path=True):
        if key in self.output:
            logging.error("In module %s, the output key '%s' is defined multiple time!" % (self.module_id, key))
            raise RuntimeError("Output key '%s' has been defined multiple times!" % key)

        if is_path:
            # Enforce output to be in platform workspace
            value = value.replace(self.output_dir, "")
            value = os.path.join(self.output_dir, value)

        self.output[key] = value

    def add_output_variable(self, key, value):
        # Declare a variable whose value will be set by the module
        if key in self.output:
            logging.error("In module %s, the output key '%s' is defined multiple time!" % (self.module_id, key))
            raise RuntimeError("Output key '%s' has been defined multiple times!" % key)
        self.output[key] = value

    def add_output_file(self, key, path, **kwargs):
        # Declare an output file that will be created by the module
        if key in self.output:
            logging.error("In module %s, the output key '%s' is defined multiple time!" % (self.module_id, key))
            raise RuntimeError("Output key '%s' has been defined multiple times!" % key)

        file_id = "%s.%s" % (self.module_id, key)
        self.output[key] = GAPFile(file_id, key, path, **kwargs)

    def get_command(self, output_dir=None):

        # Check that all required inputs are set
        err = False
        for req_type in self.required:
            if self.arguments[req_type] is None:
                logging.error("Module of type %s with id %s missing required input type: %s" % (self.__class__.__name__, self.module_id, req_type))
                err = True
        if err:
            # Raise error if any required arguments have not been set
            raise RuntimeError("Module could not generate command! Required inputs missing at runtime!")

        # Define the names of output files given an output directory (default: self.output_dir)
        self.define_output(output_dir)

        # Define the command
        cmd = self.define_command()
        return cmd

    def process_cmd_output(self, out, err):
        # Function to be overriden by inheriting classes that process output from their command to set one of their outputs
        # Example: Module that determines how many lines are in a file
        pass

    def get_ID(self):
        return self.module_id

    def set_ID(self, new_id):
        self.module_id = new_id

    def get_keys(self):
        return self.input_keys, self.output_keys

    def get_arguments(self, key=None):
        if key is None:
            return self.arguments
        return self.arguments[key]

    def set_argument(self, key, value):
        # Set value for input argument
        if key not in self.arguments:
            logging.error("Attempt to set undeclared input '%s' for module with id '%s' of type %s!" % (key,
                                                                                                        self.module_id,
                                                                                                        self.__class__.__name__))
            raise RuntimeError("Attempt to set undeclared input type for module!")
        self.arguments[key] = value

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

    def get_output_dir(self):
        return self.output_dir

    def set_output_dir(self, new_output_dir):
        self.output_dir = new_output_dir

    def get_input_files(self):
        # Convenience function to return all files passed to module as input
        pass

    def get_output_files(self):
        # Convenience function to return all files produced by module as output
        pass

    def is_merger(self):
        return self.merger
