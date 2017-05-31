from GAP_interfaces import Tool

__main_class__ = "SummarizeTrimmomatic"

class SummarizeTrimmomatic(Tool):

    def __init__(self, platform, tool_id):
        super(SummarizeTrimmomatic, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = 1
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["trim_report"]
        self.output_keys    = ["summary_file"]

        self.req_tools      = ["qc_parser"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Get options from kwargs
        input           = kwargs.get("trim_report",  None)

        # Generating command to parse Trimmomatic log for trimming stats
        cmd = "%s trimmomatic -i %s > %s !LOG2!" % (self.tools["qc_parser"], input, self.output["summary_file"])

        return cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("summary_file", "trimmomatic.summary.txt")