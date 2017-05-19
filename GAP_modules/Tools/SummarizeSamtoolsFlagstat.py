from GAP_interfaces import Tool

__main_class__ = "SummarizeSamtoolsFlagstat"

class SummarizeSamtoolsFlagstat(Tool):

    def __init__(self, config, sample_data, tool_id):
        super(SummarizeSamtoolsFlagstat, self).__init__(config, sample_data, tool_id)

        self.can_split      = False

        self.nr_cpus        = 1
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["flagstat"]
        self.output_keys    = ["summary_file"]

        self.req_tools      = ["qc_parser"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Get options from kwargs
        input           = kwargs.get("flagstat",  None)

        # Generating command to parse samtools flagstat output
        cmd = "%s flagstat -i %s > %s !LOG2!" % (self.tools["qc_parser"], input, self.output["summary_file"])

        return cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("summary_file", "flagstat.summary.txt")