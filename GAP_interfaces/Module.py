import abc

from GAP_interfaces import ABCMetaEnhanced

class Module(object):
    __metaclass__ = ABCMetaEnhanced

    def __init__(self, config, sample_data):

        self.config         = config
        self.sample_data    = sample_data

        self.nr_cpus    = None
        self.mem        = None

        self.output         = None

        self.req_tools      = None
        self.req_resources  = None

        self.tools          = self.config["paths"]["tools"]
        self.resources      = self.config["paths"]["resources"]

    def get_output(self):
        return self.output

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

    @abc.abstractmethod
    def get_command(self, **kwargs):
        raise NotImplementedError("Class does not have a required \"get_command()\" method!")

    def get_nr_cpus(self):
        return self.nr_cpus

    def get_mem(self):
        return self.mem
