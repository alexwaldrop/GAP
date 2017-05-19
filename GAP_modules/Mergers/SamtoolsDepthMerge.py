from GAP_interfaces import Merger

__main_class__ = "SamtoolsDepthMerge"

class SamtoolsDepthMerge(Merger):

    def __init__(self, config, sample_data, tool_id, main_module_name=None):
        super(SamtoolsDepthMerge, self).__init__(config, sample_data, tool_id, main_module_name)

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        self.input_keys     = ["samtools_depth"]
        self.output_keys    = ["samtools_depth"]

        self.req_tools      = []
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Obtaining the arguments
        inputs = kwargs.get("samtools_depth", None)

        # Generating command for concatenating multiple files together using unix Cat command
        cat_cmd = "cat %s > %s !LOG2!" % (" ".join(inputs), self.output["samtools_depth"])

        return cat_cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("samtools_depth", "samtoolsdepth.out")