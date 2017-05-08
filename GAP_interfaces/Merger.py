import abc

from GAP_interfaces import ABCMetaEnhanced

class Merger(object):
    __metaclass__ = ABCMetaEnhanced

    def __init__(self):
        self.output = None

        self.input_keys  = None
        self.output_keys = None

    def check_init(self):
        cls_name = self.__class__.__name__

        # Generate the set of keys that are required for a class instance
        required_keys = {
            "input_keys": self.input_keys,
            "output_keys": self.output_keys,
        }

        # Check if class instance has initialized all the attributes
        for (key_name, attribute) in required_keys.iteritems():
            if attribute is None:
                raise NotImplementedError(
                    "In module %s, the attribute \"%s\" was not initialized!" % (cls_name, key_name))

    def check_input(self, provided_keys):
        return [key for key in self.input_keys if key not in provided_keys]

    def define_output(self):
        return self.output_keys

    def get_output(self):
        return self.output

    @abc.abstractmethod
    def get_command(self, **kwargs):
        raise NotImplementedError("Class does not have a required \"get_command()\" method!")
