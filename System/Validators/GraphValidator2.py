
from Validator2 import Validator

class GraphValidator(Validator):

    def __init__(self, datastore):

        super(GraphValidator, self).__init__(datastore)

    def validate(self):

        # Make sure task graph not empty
        if len(self.graph.get_tasks()) < 1:
            self.report_error("No tasks could be parsed from Graph config. Please define at least one valid task!")

        # TODO: Validate that graph is DAG (no cycles)

        returns_input = False

        # Perform checking for each task in the graph
        for task in self.graph.get_nodes().itervalues():

            # Check to see if any input is returned
            returns_input = returns_input or len(task.get_final_output_keys()) > 0

            # Check that docker image declared by task actually exists
            self.__check_docker_image(task)

            # Check task config input
            self.__check_graph_config_input(task)

            # Check task input
            self.__check_task_input(task)

            # Check final output keys
            self.__check_final_output_keys(task)

            # Checking the module attributes
            self.__check_module_attributes(task.module)

            # Checking the module arguments
            self.__check_module_arguments(task.module)

        if not returns_input:
            self.report_error("No final output keys declared for any tasks in graph! "
                              "Please specify final_output keys for at least one module in task graph config.")

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print the available reports
        self.print_reports()

        return has_errors

    def __check_docker_image(self, task):
        # Check to make sure docker image declared in graph config actually exists in resource kit
        docker_image = task.get_docker_image_id()
        if docker_image is not None and not self.resources.has_docker_image(docker_image):
            self.report_error("In task '%s', the docker image declared in the graph config '%s' does not"
                              "exist in the 'Docker' section of the resource kit! This could be a possible typo." %(task.get_ID(), docker_image))

    def __check_graph_config_input(self, task):

        # Obtain task arguments set in graph config
        config_input = task.get_graph_config_args()

        # Get required arguments for task's module
        task_args = task.module.get_arguments()

        # Check if each config key
        for config_arg, config_value in config_input.iteritems():

            # Check if key is an input key
            if config_arg not in task_args:
                self.report_warning("In task '%s', the key '%s' is set in the graph config input, however the key is not"
                                    "an input used by the module. This could be a possible typo." % (task.get_ID(), config_arg))
                break

            # Obtain the argument
            arg = task_args[config_arg]

            # Check that any resource arguments are set to an id of an actual resource of the correct type
            if arg.is_resource():

                # Get ids of all resources that match the module argument type
                possible_resources = []
                if self.resources.has_resource_type(config_arg):
                    possible_resources = self.resources.get_resources(resource_type=config_arg).keys()

                # Add any resources declared on the docker image
                docker_image_id = task.get_docker_image_id()
                if docker_image_id is not None:
                    docker_image = self.resources.get_docker_image(docker_image_id)
                    if docker_image.has_resource_type(config_arg):
                        possible_resources.extend(docker_image.get_resources(resource_type=config_arg).keys())

                # Throw error if no resources match module argument type
                if len(possible_resources) < 1:
                    self.report_error("In task '%s', the resource key '%s', no resources of this type are defined"
                                      "in the resources config file. Please define at least one resource of type "
                                      "'%s'" % (task.get_ID(), config_arg, config_arg))

                # Throw error if module argument type exists but there aren't any resources
                if config_value not in possible_resources:
                    self.report_error("In task '%s', no resource with name '%s' of type '%s' has been found. "
                                      "Please define a resource with this name in the resources config "
                                      "file." % (task.get_ID(), config_value, config_arg))

    def __check_task_input(self, task):
        # Determine if all required arguments for task will be set at runtime

        # Obtain the node ID
        task_id = task.get_ID()

        # Get task docker image
        docker_image = None if task.get_docker_image_id() is None else self.resources.get_docker_image(task.get_docker_image_id())

        # Get task inputs specified in the graph config
        config_input = task.get_graph_config_args()

        # Get parent output types available to task at runtime
        parent_tasks = [self.graph.get_task(parent_task) for parent_task in self.graph.get_parents(task_id)]
        parent_output_types = []
        for parent_task in parent_tasks:
            parent_output_types.extend(parent_task.get_output_types())

        # Get task arguments that will need to be set at runtime
        args = task.module.get_arguments()

        # Check if each argument can be found in any of the sources
        for arg_key, arg_obj in args.iteritems():

            # Assume the key is not found
            found = False

            if arg_obj.is_resource():

                # Check if resource key is defined in the resource kit
                file_resources = self.resources.get_resources()
                docker_resources = [] if docker_image is None else docker_image.get_resources()

                # Throw error if no resources of the desired type are found in resource kit
                if arg_key not in file_resources and arg_key not in docker_resources:
                    if arg_obj.is_mandatory():
                        self.report_error("In task '%s', the resource argument '%s' has no definition "
                                            "in the resource config file." % (task_id, arg_key))
                    else:
                        self.report_warning("In task '%s', the resource argument '%s' has no definition. "
                                            "Argument is not required and it will be set to its default value. "
                                            "If desired, please specify in the graph config for task '%s' which resource '%s' "
                                            "definition is needed." % (task_id, arg_key, task_id, arg_key))

                # Throw error if required resource has multiple definitions in the same docker image
                elif len(docker_resources[arg_key]) > 1 and arg_key not in config_input:
                    if arg_obj.is_mandatory():
                        self.report_error("In module '%s', the resource argument '%s' has multiple definitions in the same docker image. "
                                        "Please specify in the graph config, for task '%s', which resource '%s'"
                                        "definition is needed." % (task_id, arg_key, task_id, arg_key))
                    else:
                        self.report_warning("In module '%s', the resource argument '%s' has multiple definitions in the same docker image. "
                                            "Argument is not required and it will be set to its default value. "
                                            "If desired, please specify in the graph config for node '%s' which resource '%s' "
                                            "definition is needed." % (task_id, arg_key, task_id, arg_key))

                # Throw error if required resource has multiple definitions in the resource kit file section
                elif len(file_resources[arg_key]) > 1 and arg_key not in config_input and len(docker_resources[arg_key]) < 1:
                    if arg_obj.is_mandatory():
                        self.report_error("In module '%s', the resource argument '%s' has multiple definitions. "
                                        "Please specify in the graph config, for node '%s', which resource '%s'"
                                        "definition is needed." % (task_id, arg_key, task_id, arg_key))
                    else:
                        self.report_warning("In module '%s', the resource argument '%s' has multiple definitions. "
                                            "Argument is not required and it will be set to its default value. "
                                            "If desired, please specify in the graph config for node '%s' which resource '%s' "
                                            "definition is needed." % (task_id, arg_key, task_id, arg_key))
                # The resource was found
                else:
                    found = True

            else:

                # Define the list of resources from where the input can come
                input_sources = [parent_output_types,
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
                                  % (task_id, arg_key))

    def __check_module_attributes(self, module):

        # Obtain the module ID
        module_ID = module.get_ID()

        # Define necessary attributes in the module
        attrs = ["output_keys"]

        # Check if the module has the necessary attributes
        for attr in attrs:
            if not hasattr(module, attr):
                self.report_error("Module '%s' has no attribute '%s'. Please initialize the list of argument keys."
                                  "Ensure you specify number of CPUs and memory argument keys." % (module_ID, attr))

    def __check_module_arguments(self, module):

        # Obtain the module ID
        module_ID = module.get_ID()

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

    def __check_final_output_keys(self, task):

        # Obtain the node ID
        task_ID = task.get_ID()

        # Obtain the final output keys
        final_output_keys = task.get_final_output_keys()

        # Obtain the module output keys
        out_keys = task.module.get_output_types()

        # Check if all final output keys are present in the output keys
        for final_key in final_output_keys:
            if final_key not in out_keys:
                self.report_error("In task '%s', the specified final output type '%s' is not part of the module's output!" % (task_ID, final_key))

