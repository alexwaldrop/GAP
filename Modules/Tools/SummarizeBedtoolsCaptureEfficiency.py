from Modules import Module

class SummarizeBedtoolsCaptureEfficiency(Module):

    def __init__(self, module_id):
        super(SummarizeBedtoolsCaptureEfficiency, self).__init__(module_id)

        self.input_keys     = ["capture_bed", "qc_parser", "nr_cpus", "mem"]
        self.output_keys    = ["summary_file"]

        self.quick_command  = True

    def define_input(self):
        self.add_argument("capture_bed",    is_required=True)
        self.add_argument("qc_parser",      is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=12)
        self.add_argument("target_type")

    def define_output(self, platform, split_name=None):
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".capture.summary.txt")
        self.add_output(platform, "summary_file", summary_file)

    def define_command(self, platform):

        # Get arguments to generate QCParser arguments
        capture_bed = self.get_arguments("capture_bed").get_value()
        qc_parser   = self.get_arguments("qc_parser").get_value()
        target_type = self.get_arguments("target_type").get_value()

        # Get output file
        summary_file = self.get_output("summary_file")

        # Generating command to parse bedtools coverage output
        cmd = "%s capture -i %s" % (qc_parser, capture_bed)

        # Add option for name of target type
        if target_type is not None:
            cmd += " --targettype %s" % target_type

        # Write output to summary file
        cmd += " > %s !LOG2!" % summary_file
        return cmd
