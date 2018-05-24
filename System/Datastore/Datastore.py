import copy
import os

from GAPFile import GAPFile

class Datastore(object):
    # Object containing all available information to GAP modules at any given instance
    def __init__(self, graph, resource_kit, sample_data, platform):

        self.graph = graph
        self.resource_kit = resource_kit
        self.sample_data = sample_data
        self.platform = platform

        # Base directories for task execution (wrk) and output storage (output)
        self.__base_wrk_dir = self.platform.wrk_dir
        self.__base_output_dir = self.platform.final_output_dir

    def get_task_arg(self, task_id, arg_type):
        # Return the object that best satisfies the arg_type for a task

        # Get all objects visible to task that match the arg_type
        possible_args = self.__gather_args(task_id, arg_type)

        # Select the object using precedence rules
        final_arg = self.__select_arg(possible_args)

        # Make deep copy of argument so internal datastore values can't be touched
        final_arg = copy.deepcopy(final_arg) if final_arg is not None else final_arg

        return final_arg

    def get_task_workspace(self, task_id=None):
        # Use task information to generate unique directories for input/output files

        if task_id is None:
            wrk_dir = self.__base_wrk_dir
            tmp_output_dir = os.path.join(self.__base_output_dir, "tmp")
            final_output_dir = self.__base_output_dir

        else:
            task = self.graph.get_task(task_id)
            visible_samples = task.get_visible_samples()

            # Create subfolders for split tasks
            if task.is_split():
                task_id = task_id.replace(".", "/")

            wrk_dir = os.path.join(self.__base_wrk_dir, task_id)
            tmp_output_dir = os.path.join(self.__base_output_dir,"tmp", task_id)
            final_output_dir = os.path.join(self.__base_output_dir, task_id)

            if visible_samples is not None and len(visible_samples) <= 1 and self.sample_data.get_num_samples() <=1:
                # Single sample output of multi-sample analysis always goes in sample level folder
                sample_name = visible_samples[0]
                # Remove sample name from path if it already appears and put it as the first directory
                task_id = task_id.replace(sample_name+"/", "")
                final_output_dir = os.path.join(self.__base_output_dir, sample_name, task_id)

        # Standardize directories
        wrk_dir             = self.platform.standardize_dir(wrk_dir)
        tmp_output_dir      = self.platform.standardize_dir(tmp_output_dir)
        final_output_dir    = self.platform.standardize_dir(final_output_dir)

        # Create and return TaskWorkspace
        return TaskWorkspace(wrk_dir, tmp_output_dir, final_output_dir, task_id)

    def get_docker_image(self, docker_id):
        return self.resource_kit.get_docker_image(docker_id)

    def get_task_input_files(self, task_id):
        # Return list of input files that need to be loaded for in order for task to run

        # Get nested list of module arguments
        module = self.graph.get_tasks(task_id).get_module()
        inputs = module.get_input_values()

        # Flatten nested list into a single list
        inputs = flatten(inputs)

        # Loop through and determine which are files
        input_files = []
        for input in inputs:
            # Append input if it's a file and one that doesn't appear on the docker
            if isinstance(input, GAPFile) and not input.is_flagged("docker"):
                input_files.append(input)

        return input_files

    def get_task_output_files(self, task_id):
        # Return list of output files produced by task
        module = self.graph.get_tasks(task_id).get_module()
        outputs = module.get_output_values()

        # Flatten nested list into a single list
        outputs = flatten(outputs)

        # Loop through and determine which are files
        output_files = []
        for output in outputs:
            # Append if output is an instance of
            if isinstance(output, GAPFile):
                output_files.append(output)

        # Return actual copies so that module paths get updated as they are transferred
        return output_files

    def __select_arg(self, avail_args):
        # Priority of checking for argument
        input_order = ["parent_input", "docker_input", "resource_input", "sample_input", "config_input"]

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

        # Get args from docker requested by task
        possible["docker_input"] = self.__gather_docker_args(task_id, arg_type)

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

    def __gather_docker_args(self, task_id, arg_type):

        args = []
        docker_image_id = self.graph.get_tasks(task_id).get_docker_image_id()

        if docker_image_id is not None and self.resource_kit.has_docker_image(docker_image_id):
            docker_image = self.resource_kit.get_docker_image(docker_image_id)
            if docker_image.has_resource_type(arg_type):

                # Search to see if the argument key appears in the config input
                config_input = self.graph.get_tasks(task_id).get_graph_config_args()
                if arg_type in config_input:

                    res_name = config_input[arg_type]

                    # Get the resource object with name "res_name" from docker input data
                    args = [docker_image.get_resources(arg_type)[res_name]]

                # If not in config input, shoudl only be one resource of type "arg_key" in docker
                else:
                    args = [self.resource_kit.get_resources(arg_type).values()[0]]

        return args

class TaskWorkspace(object):
    # Defines folder structure where task will execute/files generated
    def __init__(self, wrk_dir, tmp_output_dir, final_output_dir, task_id):
        # Work dir: Folder where input/output files will be generated by task
        # tmp_output_dir: Folder where temporary final output will be saved until all tasks are finished
        # final_output_dir: Folder where final output files will be saved
        self.workspace = {"wrk" : wrk_dir,
                          "tmp_output" : tmp_output_dir,
                          "final_output" : final_output_dir}

        # Define wrk/final log directories
        self.workspace["wrk_log"] = os.path.join(wrk_dir, "log/")
        self.workspace["final_log"] = os.path.join(final_output_dir, "log/")

        # Convert directories to GAPFiles
        for dir_type, dir_path in self.workspace.iteritems():
            # Attempt to create a unique ID if task_id is given
            dir_id = dir_type if task_id is None else "%s_%s" % (task_id, dir_type)
            self.workspace[dir_type] = GAPFile(file_id=dir_id, file_type=dir_type, path=dir_path)

    def get_wrk_dir(self):
        return self.workspace["wrk"]

    def get_output_dir(self):
        return self.workspace["final_output"]

    def get_tmp_output_dir(self):
        return self.workspace["tmp_output"]

    def get_wrk_log_dir(self):
        return self.workspace["wrk_log"]

    def get_final_log_dir(self):
        return self.workspace["final_log"]


# Method for unpacking nested list taken from http://code.activestate.com/recipes/578948-flattening-an-arbitrarily-nested-list-in-python/
def flatten(lis):
    """Given a list, possibly nested to any level, return it flattened."""
    new_lis = []
    for item in lis:
        if type(item) == type([]):
            new_lis.extend(flatten(item))
        else:
            new_lis.append(item)
    return new_lis

