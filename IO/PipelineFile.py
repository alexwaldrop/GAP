from PlatformFile import PlatformFile

class PipelineFile(PlatformFile):
    # Class to hold information for pipeline input/output files
    def __init__(self, name, path, **kwargs):

        # ID of node that produced file
        self.node_id        = kwargs.pop("node_id",     None)

        # Name of module that produced file
        self.module_name    = kwargs.pop("module_name", None)

        # Set whether output is from a split, main, or merge module
        self.is_merge_output    = kwargs.pop("is_merge_output", False)

        self.is_main_output     = kwargs.pop("is_main_output",  False)

        self.is_split_output    = kwargs.pop("is_split_output", False)

        self.is_main_input      = kwargs.pop("is_main_input",   False)

        # Call super to inherit from platform file
        super(PipelineFile, self).__init__(name, path, **kwargs)

    def get_node_id(self):
        return self.node_id

    def get_module_name(self):
        return self.module_name

    def is_split_output(self):
        return self.is_split_output

    def is_main_output(self):
        return self.is_main_output

    def is_merge_output(self):
        return self.is_merge_output

    def is_main_input(self):
        return self.is_main_input

    def set_node_id(self, new_node_id):
        self.node_id = new_node_id

    def set_module_name(self, new_module_name):
        self.module_name = new_module_name


