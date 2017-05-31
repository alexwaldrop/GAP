import os
from GAP_interfaces import Tool

__main_class__ = "SummaryMerge"

class SummaryMerge(Tool):

    def __init__(self, platform, tool_id):
        super(SummaryMerge, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = 1
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["summary_file"]
        self.output_keys    = ["summary_file"]

        self.req_tools      = ["qc_parser"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Get options from kwargs
        summary_files  = kwargs.get("summary_file",  None)
        sample_name    = kwargs.get("sample_name", self.pipeline_data.get_sample_name())

        # Generating command to merge QC summary output files from two or more QCParser modules
        cmd = "%s merge -i %s --sample %s > %s !LOG2!" % \
              (self.tools["qc_parser"], " ".join(summary_files), sample_name, self.output["summary_file"])

        return cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("summary_file", "full_qc.summary.txt")