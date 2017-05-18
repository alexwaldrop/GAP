from GAP_interfaces import Tool

__main_class__ = "SummarizePicardInsertSizeMetrics"

class SummarizePicardInsertSizeMetrics(Tool):

    def __init__(self, config, sample_data):
        super(SummarizePicardInsertSizeMetrics, self).__init__(config, sample_data)

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

        # Set name of output file
        output = "%s.insertsummary.txt" % input.split(".")[0]

        # Generating command to parse picard CollectInsertSizeMetrics output
        cmd = "%s insertsize -i %s > %s !LOG2!" % (self.tools["qc_parser"], input, output)

        # Generating the output
        self.output = dict()
        self.output["summary_file"] = output

        return cmd