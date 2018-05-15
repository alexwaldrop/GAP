import logging
import os
from collections import OrderedDict

from System.Datastore import GAPFile
from Module import Module

class Splitter(Module):

    def __init__(self, module_id):

        # Initialize object
        super(Splitter, self).__init__(module_id)
        self.output = OrderedDict()

    def define_input(self):
        raise NotImplementedError(
            "Splitter module %s must implement 'define_input()' function!" % self.__class__.__name__)

    def define_output(self):
        raise NotImplementedError(
            "Splitter module %s must implement 'define_output()' function!" % self.__class__.__name__)

    def define_command(self):
        raise NotImplementedError(
            "Splitter module %s must implement 'define_command()' function!" % self.__class__.__name__)

    def make_split(self, split_id, visible_samples=None):
        # Create new split with id and the samples visible (None=all samples)
        if split_id in self.output:
            logging.error("Module of type '%s' declared split with duplicate id (%s)!" % (self.__class__.__name__, split_id))
            raise RuntimeError("Module declared split with duplicate id!")

        # Convert visible samples to list if its a string so we can always assume its a list
        if isinstance(visible_samples, basestring):
            visible_samples = [visible_samples]

        # Create split with the following samples visible
        self.output[split_id] = {"visible_samples" : visible_samples}

    def add_output_variable(self, split_id, key, value):
        # Declare a variable whose value will be set by the module
        if key in self.output[split_id]:
            logging.error("In module %s, the output key '%s' is defined multiple times in same split (split_id: %s)!" % (self.module_id, key, split_id))
            raise RuntimeError("Output key '%s' has been defined multiple times within the same split!" % key)
        self.output[split_id][key] = value

    def add_output_file(self, split_id, key, path, **kwargs):
        # Declare an output file that will be created by the module
        if key in self.output[split_id]:
            logging.error("In module %s, the output key '%s' is defined multiple times in same split (split_id: %s)!" % (self.module_id, key, split_id))
            raise RuntimeError("Output key '%s' has been defined multiple times within the same split!" % key)

        file_id = "%s.%s.%s" % (self.module_id, split_id, key)
        self.output[split_id][key] = GAPFile(file_id, key, path, **kwargs)

    def get_output(self, key=None, split_id=None):
        if split_id is None:
            return self.output
        elif key is None:
            return self.output[split_id]
        return self.output[split_id][key]

    def set_output(self, split_id, key, value):
        # Set value for output object
        if key not in self.output_keys:
            logging.error("Attempt to set undeclared output type '%s' for module with id '%s' of type %s" % (key,
                                                                                                             self.module_id,
                                                                                                             self.__class__.__name__))
            raise RuntimeError("Attempt to set undeclared output type for module!")

        if split_id not in self.output:
            logging.error("Attempt to set undeclared output type '%s' for module with id '%s' of type %s" % (key,
                                                                                                             self.module_id,
                                                                                                             self.__class__.__name__))
            raise RuntimeError("Attempt to set undeclared output type for module!")
        self.output[split_id][key] = value

    def generate_unique_file_name(self, split_id, extension=".dat", output_dir=None):

        # Generate file basename
        path = "%s.%s" % (self.module_id, split_id)

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

    def get_input_files(self):
        # Convenience function to return all files passed to module as input
        pass

    def get_output_files(self):
        # Convenience function to return all files produced by module as output
        pass
