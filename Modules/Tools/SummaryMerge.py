from Modules import Module

class SummaryMerge(Module):

    def __init__(self, module_id):
        super(SummaryMerge, self).__init__(module_id)

        self.input_keys     = ["summary_file", "qc_parser", "nr_cpus", "mem", "sample_name"]
        self.output_keys    = ["summary_file"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("summary_file",       is_required=True)
        self.add_argument("qc_parser",          is_required=True, is_resource=True)
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=1)
        self.add_argument("mem",                is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".full_qc.summary.txt")
        self.add_output(platform, "summary_file", summary_file)

    def define_command(self, platform):
        # Get options from kwargs
        inputs          = self.get_arguments("summary_file").get_value()
        qc_parser       = self.get_arguments("qc_parser").get_value()
        sample_name     = self.get_arguments("sample_name")
        summary_file    = self.get_output("summary_file")

        # Generating command to merge QC summary output files from two or more QCParser modules
        cmd = "%s merge -i %s --sample %s > %s !LOG2!" % (qc_parser,
                                                          " ".join(inputs),
                                                          sample_name,
                                                          summary_file)
        return cmd
