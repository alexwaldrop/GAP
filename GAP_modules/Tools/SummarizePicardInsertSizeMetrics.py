from GAP_interfaces import Tool

__main_class__ = "SummarizePicardInsertSizeMetrics"

class SummarizePicardInsertSizeMetrics(Tool):

    def __init__(self, platform, tool_id):
        super(SummarizePicardInsertSizeMetrics, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = 1
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["insert_size_report"]
        self.output_keys    = ["summary_file"]

        self.req_tools      = ["qc_parser"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Get options from kwargs
        input           = kwargs.get("insert_size_report",  None)

        # Generating command to parse picard CollectInsertSizeMetrics output
        cmd = "%s insertsize -i %s > %s !LOG2!" % (self.tools["qc_parser"], input, self.output["summary_file"])

        return cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("summary_file", "insertsize.summary.txt")
