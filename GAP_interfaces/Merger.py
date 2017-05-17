import abc

from GAP_interfaces import ABCMetaEnhanced

class Merger(object):
    __metaclass__ = ABCMetaEnhanced

    def __init__(self, config, sample_data):

        self.config = config
        self.sample_data = sample_data

        self.output = None

        self.input_keys  = None
        self.output_keys = None

        self.req_tools = None
        self.req_resources = None

        self.tools      = self.config["paths"]["tools"]
        self.resources  = self.config["paths"]["resources"]

    def check_init(self):
        cls_name = self.__class__.__name__

        # Generate the set of keys that are required for a class instance
        required_keys = {
            "input_keys": self.input_keys,
            "output_keys": self.output_keys,

            "req_tools":       self.req_tools,
            "req_resources":   self.req_resources,
        }

        # Check if class instance has initialized all the attributes
        for (key_name, attribute) in required_keys.iteritems():
            if attribute is None:
                raise NotImplementedError(
                    "In module %s, the attribute \"%s\" was not initialized!" % (cls_name, key_name))

    def check_input(self, provided_keys):
        return [key for key in self.input_keys if key not in provided_keys]

    def check_requirements(self):

        # Generating the not found lists
        not_found = dict()
        not_found["tools"] = []
        not_found["resources"] = []

        # Identifying if all the required tool keys are found in the config object
        for req_tool_key in self.req_tools:
            if req_tool_key not in self.tools:
                not_found["tools"].append(req_tool_key)

        # Identifying if all the required resource keys are found in the config object
        for req_res_key in self.req_resources:
            if req_res_key not in self.resources:
                not_found["resources"].append(req_res_key)

        return not_found

    def define_output(self):
        return self.output_keys

    def get_output(self):
        return self.output

    @abc.abstractmethod
    def get_command(self, **kwargs):
        raise NotImplementedError("Class does not have a required \"get_command()\" method!")
