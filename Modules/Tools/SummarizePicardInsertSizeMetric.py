from Modules import Module

class SummarizePicardInsertSizeMetrics(Module):

    def __init__(self, module_id):
        super(SummarizePicardInsertSizeMetrics, self).__init__(module_id)

        self.input_keys     = ["insert_size_report", "qc_parser", "nr_cpus", "mem"]
        self.output_keys    = ["summary_file"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("insert_size_report", is_required=True)
        self.add_argument("qc_parser",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default=1)
        self.add_argument("mem",                is_required=True, default=1)

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".insertsize.summary.txt")
        self.add_output(platform, "summary_file", summary_file)

    def define_command(self, **kwargs):
        # Get options from kwargs
        input           = self.get_arguments("insert_size_report").get_value()
        qc_parser       = self.get_arguments("qc_parser").get_value()
        summary_file    = self.get_output("summary_file").get_value()

        # Generating command to parse picard CollectInsertSizeMetrics output
        cmd = "%s insertsize -i %s > %s !LOG2!" % (qc_parser, input, summary_file)
        return cmd