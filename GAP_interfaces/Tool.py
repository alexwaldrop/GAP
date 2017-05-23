from GAP_interfaces import Module

class Tool(Module):

    def __init__(self, config, sample_data, tool_id):
        super(Tool, self).__init__(config, sample_data, tool_id)

        self.can_split = None
        self.splitter = None
        self.merger = None

        self.input_keys = None
        self.splitted_input_keys = None
        self.output_keys = None
        self.splitted_output_keys = None

        self.main_module_name = self.__class__.__name__

    def check_init(self):
        cls_name = self.__class__.__name__

        # Generate the set of keys that are required for a class instance (both normal and splitted mode)
        required_keys = {
            "nr_cpus":      self.nr_cpus,
            "mem":          self.mem,
            "can_split":    self.can_split,
            "input_keys":   self.input_keys,
            "output_keys":  self.output_keys,

            "req_tools":       self.req_tools,
            "req_resources":   self.req_resources,
        }

        # Check if class instance has initialized all the attributes
        for (key_name, attribute) in required_keys.iteritems():
            if attribute is None:
                raise NotImplementedError(
                    "In module %s, the attribute \"%s\" was not initialized!" % (cls_name, key_name))

        # Generate the set of keys that are required for a class instance only in splitted mode
        if self.can_split:
            required_keys = {
                "splitter":             self.splitter,
                "merger":               self.merger,
                "splitted_input_keys":  self.splitted_input_keys,
                "splitted_output_keys": self.splitted_output_keys,
            }

            # Check if class instance has initialized all the attributes
            for (key_name, attribute) in required_keys.iteritems():
                if attribute is None:
                    raise NotImplementedError(
                        "In module %s, the splitted mode attribute \"%s\" was not initialized!" % (cls_name, key_name))

    def check_input(self, provided_keys, splitted=False):
        if splitted and self.can_split:
            search_list = self.splitted_input_keys
        else:
            search_list = self.input_keys

        return [ key for key in search_list if key not in provided_keys ]

    def define_output(self, splitted=False):
        if splitted and self.can_split:
            return self.splitted_output_keys
        else:
            return self.output_keys