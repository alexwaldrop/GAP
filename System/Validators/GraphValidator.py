
from System.Validators import Validator

class GraphValidator(Validator):

    def __init__(self, pipeline_obj):

        super(GraphValidator, self).__init__(pipeline_obj)

    def __check_config_input(self, node):

        # Obtain the config input of the node
        config_input = node.get_config_input()

        # Obtain the input args from the modules present in the node
        node_args = {}
        node_args.update( node.split_module.get_arguments() if node.is_split_mode() else {} )
        node_args.update( node.main_module.get_arguments() )
        node_args.update( node.merge_module.get_arguments() if node.is_split_mode() else {} )

        # Check if each config key
        for config_arg, config_value in config_input.iteritems():

            # Check if key is an input key
            if config_arg not in node_args:
                self.report_warning("In node '%s', the key '%s' is set in the config input, however the key is not"
                                    "an input key. This could be a possible typo." % (node.get_ID(), config_arg))
                break

            # Obtain the argument
            arg = node_args[config_arg]

            # Check if the key is a resource and it is valid
            if arg.is_resource():

                # Check any resource of type config_arg is defined
                if not self.resources.has_resource_type(config_arg):
                    self.report_error("In node '%s', the resource key '%s', no resources of this type are defined"
                                      "in the resources config file. Please define at least one resource of type "
                                      "'%s'" % (node.get_ID(), config_arg, config_arg))
                    break

                # Check if the value present in the config is a valid resource name
                possible_resources = self.resources.get_resources(resource_type=config_arg)
                if config_value not in possible_resources:
                    self.report_error("In node '%s', no resource with name '%s' of type '%s' has been found. "
                                      "Please define a resource with this name in the resources config "
                                      "file." % (node.get_ID(), config_value, config_arg))

    def __check_module_input(self, node_id, module, module_input_keys=None, node_input_keys=None, config_input=None):

        # Obtain the arguments that need to be set
        args = module.get_arguments()

        # Check if each argument can be found in any of the sources
        for arg_key, arg_obj in args.iteritems():

            # Assume the key is not found
            found = False

            if arg_obj.is_resource():

                # Check if resource key is defined in the resource kit
                resources = self.resources.get_resources()
                if arg_key not in resources:
                    if arg_obj.is_mandatory():
                        self.report_error("In module '%s', the resource argument '%s' has no definition "
                                            "in the resource config file." % (module.get_ID(), arg_key))
                    else:
                        self.report_warning("In module '%s', the resource argument '%s' has no definition. "
                                            "Argument is not required and it will be set to its default value. "
                                            "If desired, please specify in the graph config for node '%s' which resource '%s' "
                                            "definition is needed." % (module.get_ID(), arg_key, node_id, arg_key))

                # Check if the resource type has more then one definitions, so the user has to select one
                elif len(resources[arg_key]) > 1 and arg_key not in config_input:
                    if arg_obj.is_mandatory():
                        self.report_error("In module '%s', the resource argument '%s' has multiple definitions. "
                                        "Please specify in the graph config, for node '%s', which resource '%s'"
                                        "definition is needed." % (module.get_ID(), arg_key, node_id, arg_key))
                    else:
                        self.report_warning("In module '%s', the resource argument '%s' has multiple definitions. "
                                            "Argument is not required and it will be set to its default value. "
                                            "If desired, please specify in the graph config for node '%s' which resource '%s' "
                                            "definition is needed." % (module.get_ID(), arg_key, node_id, arg_key))
                # The resource was found
                else:
                    found = True

            else:

                # Define the list of resources from where the input can come
                input_sources = [module_input_keys,
                                 node_input_keys,
                                 self.samples.get_data(),
                                 config_input]

                # Check if the key is found in any of the above input sources
                for input_source in input_sources:
                    if input_source is not None and arg_key in input_source:
                        found = True
                        break

            # Skip if the argument key has been found
            if found:
                continue

            # Skip if there is a default value available
            if arg_obj.get_default_value() is not None:
                continue

            # If still not found, check if the key is required
            if arg_obj.is_mandatory():
                self.report_error("In module '%s', the defined argument '%s' will not be set during runtime. "
                                  "Please check the graph and resources definition and configuration files."
                                  % (module.get_ID(), arg_key))

    def __check_node_input(self, node):

        # Obtain the node ID
        node_id = node.get_ID()

        # Obtain nodes
        nodes = self.graph.get_nodes()

        # Obtain the output keys from the parent nodes of the current node
        input_nodes = self.graph.get_adjacency_list()[ node.get_ID() ]
        keys_from_nodes = []
        for node_id in input_nodes:
            keys_from_nodes.extend(nodes[node_id].get_output_keys())

        # Obtain the config input from the graph config
        config_input = node.get_config_input()

        # Check if all the arguments can be set in split mode
        if node.is_split_mode():

            # Obtain the splitter module
            split_module = node.split_module

            # Test the splitter module with the available input keys
            self.__check_module_input(node_id, split_module,
                                      node_input_keys=keys_from_nodes,
                                      config_input=config_input)

            # Obtain the output keys from the splitter
            _, splitter_keys = split_module.get_keys()

            # Obtain the main module
            main_module = node.main_module

            # Test the main module with the available input_keys
            self.__check_module_input(node_id, main_module,
                                      module_input_keys=splitter_keys,
                                      node_input_keys=keys_from_nodes,
                                      config_input=config_input)

            # Obtain the output keys from the main module
            _, main_keys = main_module.get_keys()

            # Obtain the merger module
            merge_module = node.merge_module

            # Test the merger module with the available input keys
            self.__check_module_input(node_id, merge_module,
                                      module_input_keys=main_keys,
                                      node_input_keys=keys_from_nodes,
                                      config_input=config_input)

        else:

            # Obtain the main module
            main_module = node.main_module

            # Test the main module with the available input keys
            self.__check_module_input(node_id, main_module,
                                      node_input_keys=keys_from_nodes,
                                      config_input=config_input)

    def __check_module_attributes(self, module):

        # Obtain the module ID
        module_ID = module.get_ID()

        # Define necessary attributes in the module
        attrs = ["input_keys", "output_keys"]

        # Check if the module has the necessary attributes
        for attr in attrs:
            if not hasattr(module, attr):
                self.report_error("Module '%s' has no attribute '%s'. Please initialize the list of argument keys."
                                  "Ensure you specify number of CPUs and memory argument keys." % (module_ID, attr))

    def __check_module_arguments(self, module):

        # Obtain the module ID
        module_ID = module.get_ID()

        # Obtain module input keys
        input_keys, _ = module.get_keys()

        # Obtain the list of arguments from the module
        args = module.get_arguments()

        # Define necessary arguments in the modules
        arg_keys = {"nr_cpus": "number of CPUs",
                    "mem": "memory requirements"}

        # Check if the module has the necessary arguments
        for arg_key in arg_keys:
            if arg_key not in args:
                self.report_error("Module '%s' has no argument '%s'. Please add an argument that defines the %s for"
                                  "the module" % (module_ID, arg_key, arg_keys[arg_key]))

        # Check if all the input keys are specified in the arguments
        for input_key in input_keys:
            if input_key not in args:
                self.report_warning("Module '%s' has defined '%s' as an input key. However, no argument with this key"
                                    "was declared." % (module_ID, input_key))

    def __check_final_output_keys(self, node):

        # Obtain the node ID
        node_ID = node.get_ID()

        # Obtain the final output keys
        final_output_keys = node.get_final_output_keys()

        # Obtain the output keys
        if node.is_split_mode():
            out_keys = node.merge_module.get_keys()[1]
            out_keys.extend(key for key in node.main_module.get_keys()[1] if key not in out_keys)
            out_keys.extend(key for key in node.split_module.get_keys()[1] if key not in out_keys)
        else:
            out_keys = node.main_module.get_keys()[1]

        # Check if all final output keys are present in the output keys
        for final_key in final_output_keys:
            if final_key not in out_keys:
                self.report_error("In node '%s', the specified final output key '%s' is not part of the output keys "
                                  "of the node." % (node_ID, final_key))

    def validate(self):

        # Perform checking for each node in the graph
        for node in self.graph.get_nodes().itervalues():

            # Check node config input
            self.__check_config_input(node)

            # Check the node input
            self.__check_node_input(node)

            # Check final output keys
            self.__check_final_output_keys(node)

            # Define the list of modules to check
            if node.is_split_mode():
                modules = [node.split_module, node.main_module, node.merge_module]
            else:
                modules = [node.main_module]

            for module in modules:

                # Checking the module attributes
                self.__check_module_attributes(module)

                # Checking the module arguments
                self.__check_module_arguments(module)

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print the available reports
        self.print_reports()

        return has_errors