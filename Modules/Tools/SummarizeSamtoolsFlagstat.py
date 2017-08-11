from Modules import Module

class SummarizeSamtoolsFlagstat(Module):

    def __init__(self, module_id):
        super(SummarizeSamtoolsFlagstat, self).__init__(module_id)

        self.input_keys     = ["flagstat", "qc_parser", "nr_cpus", "mem"]
        self.output_keys    = ["summary_file"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("flagstat",   is_required=True)
        self.add_argument("qc_parser",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default=1)
        self.add_argument("mem",        is_required=True, default=1)

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".flagstat.summary.txt")
        self.add_output(platform, "summary_file", summary_file)

    def define_command(self, platform):

        # Get options from kwargs
        input           = self.get_arguments("flagstat").get_value()
        qc_parser       = self.get_arguments("qc_parser").get_value()
        summary_file    = self.get_output("summary_file")

        # Generating command to parse samtools flagstat output
        cmd = "%s flagstat -i %s > %s !LOG2!" % (qc_parser, input, summary_file)
        return cmd
