from Modules import Module

class ParseTrimmomatic(Module):

    def __init__(self, module_id):
        super(ParseTrimmomatic, self).__init__(module_id)

        self.input_keys     = ["trim_report", "qc_parser", "nr_cpus", "mem"]
        self.output_keys    = ["qc_report"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("trim_report",        is_required=True)
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("qc_parser",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=1)
        self.add_argument("mem",                is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".trimmomatic.qc_report.txt")
        self.add_output(platform, "qc_report", summary_file)

    def define_command(self, platform):
        # Get options from kwargs
        input_file      = self.get_arguments("trim_report").get_value()
        qc_parser       = self.get_arguments("qc_parser").get_value()
        sample_name     = self.get_arguments("sample_name").get_value()
        qc_report       = self.get_output("qc_report")

        # Generating command to parse Trimmomatic log for trimming stats
        cmd = "%s Trimmomatic -i %s -s %s > %s !LOG2!" % (qc_parser, input_file, sample_name, qc_report)
        return cmd
