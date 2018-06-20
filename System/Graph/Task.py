import logging
import importlib
import copy

from Modules import Splitter, Merger

class Task(object):

    def __init__(self, task_id, **kwargs):

        self.__task_id              = task_id

        # Get the module names
        self.__module_name          = kwargs.pop("module")

        # Get the final output keys
        self.__final_output_keys    = kwargs.pop("final_output")

        # Id of docker image declared in Resource kit where task will be executed
        self.__docker_image         = kwargs.pop("docker_image", None)

        # Get the config inputs
        self.__module_args          = kwargs.pop("args", [])

        # Initialize modules
        self.module                 = self.__load_module(self.__module_name, is_docker=self.__docker_image is not None)

        # Whether task has been completed
        self.complete   = False

        # ID of task that cloned current task
        self.__clones = []

        # Boolean for whether task was created by splitting an existing task
        self.__is_split = False

        # Upstream splitter task that created split task
        self.__splitter = None

        # Output partition visible to task
        self.__split_id = None

        # Sample partition visible to task
        self.__visible_samples = None

        # Flag for whether task has been split/replaced and shouldn't be executed
        self.__deprecated = False

    def split(self, splitter_id, split_id, visible_samples):
        # Produce clone of current task but restrict visible output and sample info available to task
        # Visible output/sample partition defined by upstream splitting task
        # Splitter is the name of the head task that created the split of current task
        # Split_id is the name of the partition the newly created task will be able to access
        # visible_samples is list of samples visible to new split

        # Create copy of current task and give new id
        split_task = copy.deepcopy(self)
        new_id = "%s.%s" % (self.__task_id, split_id)
        split_task.__task_id = new_id

        # Add daughter split to list of clones spawned from current task
        self.__clones.append(new_id)

        split_task.__clones = []

        # Upstream task responsible for creating new split task
        split_task.__splitter = splitter_id

        # Split visible to this split task
        split_task.__split_id = split_id

        # Samples visible to split task
        split_task.__visible_samples = visible_samples

        # Specify that new split task is the result of a split
        split_task.__is_split = True

        # Give new module id to module
        split_task.module.set_ID(new_id)

        # Remove deprecated flag possibly inherited from parent
        split_task.__deprecated = False

        return split_task

    def get_ID(self):
        return self.__task_id

    def get_module(self):
        return self.module

    def is_splitter_task(self):
        return isinstance(self.module, Splitter)

    def is_merger_task(self):
        return isinstance(self.module, Merger)

    def get_input_args(self):
        return self.module.get_arguments()

    def get_input_keys(self):
        # Return input keys. get_keys() returns input_keys and output_keys.
        return self.module.get_input_types()

    def get_output_keys(self):
        # Return output keys. get_keys() returns input_keys and output_keys.
        return self.module.get_output_types()

    def get_final_output_keys(self):
        return self.__final_output_keys

    def get_graph_config_args(self):
        return self.__module_args

    def get_docker_image_id(self):
        return self.__docker_image

    def set_complete(self, is_complete):
        self.complete = is_complete

    def is_complete(self):
        return self.complete

    def is_split(self):
        return self.__is_split

    def get_visible_samples(self):
        return self.__visible_samples

    def get_splitter(self):
        return self.__splitter

    def get_split_id(self):
        return self.__split_id

    def deprecate(self):
        self.__deprecated = True

    def is_deprecated(self):
        return self.__deprecated

    def get_clones(self):
        return self.__clones

    def __load_module(self, module_name, is_docker):

        # Try importing the module
        try:
            _module = importlib.import_module(module_name)
        except:
            logging.error("Module %s could not be imported! "
                          "Check the module name spelling and ensure the module exists." % module_name)
            raise

        # Get the class
        _class = _module.__dict__[module_name]

        # Generate the module ID
        module_id = "%s_%s" % (self.__task_id, module_name)

        return _class(module_id, is_docker)

    def get_task_string(self, input_from=None):
        # Get the module names
        to_ret = "[%s]\n" % self.__task_id
        to_ret +="\tmodule\t= %s\n" % self.module.__class__.__name__

        if len(self.__final_output_keys) == 1:
            to_ret += "\tfinal_output\t= %s\n" % self.__final_output_keys[0]
        elif len(self.__final_output_keys) > 1:
            to_ret += "\tfinal_output\t= %s\n" % ",".join(self.__final_output_keys)

        if self.__docker_image is not None:
            to_ret += "\tdocker_image\t= %s\n" % self.__docker_image

        if isinstance(input_from, list) and len(input_from) == 1:
            to_ret += "\tinput_from\t= %s\n" % input_from[0]

        elif isinstance(input_from, list) and len(input_from) > 1:
            to_ret += "\tinput_from\t= %s\n" % ",".join(input_from)

        to_ret += "\tis_complete\t= %s\n" % self.complete
        to_ret += "\tis_split\t= %s\n" % self.__is_split
        to_ret += "\tsplitter_task\t= %s\n" % self.__splitter
        to_ret += "\tsplit_id\t= %s\n" % self.__split_id

        if isinstance(self.__visible_samples, list):
            if len(self.__visible_samples) == 1:
                to_ret += "\tvisible_samples\t= %s\n" % self.__visible_samples
            else:
                to_ret += "\tvisible_samples\t= %s\n" % ",".join(self.__visible_samples)

        to_ret += "\tdeprecated\t= %s\n" % self.__deprecated

        if len(self.__module_args) > 0:
            to_ret += "\t[[args]]\n"
            for key in self.__module_args:
                to_ret += "\t\t%s\t= %s\n" % (key, self.__module_args[key])

        return to_ret
