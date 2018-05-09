import copy

class Datastore(object):
    # Object containing all available information to GAP modules at any given instance
    def __init__(self, graph, resource_kit, sample_data):

        self.graph = graph
        self.resource_kit = resource_kit
        self.sample_data = sample_data

    def get_task_arg(self, task_id, arg_type):
        # Return the object that best satisfies the arg_type for a task

        # Get all objects visible to task that match the arg_type
        possible_args = self.__gather_args(task_id, arg_type)

        # Select the object using precedence rules
        final_arg = self.__select_arg(possible_args)

        # Make deep copy of argument so internal datastore values can't be touched
        final_arg = copy.deepcopy(final_arg) if final_arg is not None else final_arg

        return final_arg

    def __select_arg(self, avail_args):
        # Priority of checking for argument
        input_order = ["parent_input", "resource_input", "sample_input", "config_input"]

        # Search the key in each input type
        for input_type in input_order:
            # List of values matching type
            if len(avail_args[input_type]) > 1:
                return avail_args[input_type]
            # Single value matching type
            if len(avail_args[input_type]) > 0:
                return avail_args[input_type][0]
        return None

    def __gather_args(self, task_id, arg_type):
        # Gather possible inputs to a task matching arg_type
        possible = {}

        # Get args from parent tasks
        possible["parent_input"] = self.__gather_parent_args(task_id, arg_type)

        # Get args from resource kit
        possible["resource_input"] = self.__gather_res_kit_args(task_id, arg_type)

        # Get args from sample data
        possible["sample_input"] = self.__gather_sample_args(task_id, arg_type)

        # Get args from config input
        config_input = self.graph.get_tasks(task_id).get_graph_config_args()
        possible["config_input"] = [] if arg_type not in config_input else config_input[arg_type]

        return possible

    def __gather_parent_args(self, task_id, arg_type):
        # Get args of specified type inherited from parent tasks
        args = []
        curr_task = self.graph.get_tasks(task_id)
        for parent_id in self.graph.get_parents(task_id):
            parent = self.graph.get_tasks(parent_id)
            if parent.is_splitter_task():
                # Limit output to partition visible to task
                split_id = curr_task.get_split_id()
                output = parent.get_output(split_id=split_id)
            else:
                output = parent.get_output()
            if arg_type in output:
                args.append(output[arg_type])
        return args

    def __gather_sample_args(self, task_id, arg_type):
        # Get args of specified type from sample data
        args = []
        # Get list of samples visible to current task
        curr_task = self.graph.get_task(task_id)
        visible_samples = curr_task.get_visible_samples()
        if self.sample_data.has_data_type(arg_type):

            # Restrict sample sheet access to visible samples if necessary
            if visible_samples is not None:
                args = self.sample_data.get_data(arg_type, samples=visible_samples)

            # Return variables from all samples
            else:
                args = self.sample_data.get_data(arg_type)

        return args

    def __gather_res_kit_args(self, task_id, arg_type):

        args = []

        if self.resource_kit.has_resource_type(arg_type):

            # Search to see if the argument key appears in the config input
            config_input = self.graph.get_tasks(task_id).get_graph_config_args()
            if arg_type in config_input:
                # Obtain the resource name
                res_name = config_input[arg_type]

                # Get the resource object with name "res_name" from resource input data
                args = [self.resource_kit.get_resources(arg_type)[res_name]]

            # If not in config input, should only be one resource of type "arg_key"
            else:
                # There should be only one resource of type "arg_key"
                args = [self.resource_kit.get_resources(arg_type).values()[0]]

        return args
