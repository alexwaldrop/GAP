from Modules import Module

class ParseBedtoolsIntersect(Module):

    def __init__(self, module_id):
        super(ParseBedtoolsIntersect, self).__init__(module_id)

        self.input_keys     = ["capture_bed", "qc_parser", "nr_cpus", "mem"]
        self.output_keys    = ["qc_report"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("capture_bed",    is_required=True)
        self.add_argument("qc_parser",      is_required=True, is_resource=True)
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=12)

    def define_output(self, platform, split_name=None):
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".bedintersect.qc_report.txt")
        self.add_output(platform, "qc_report", summary_file)

    def define_command(self, platform):

        # Get arguments to generate QCParser arguments
        capture_bed = self.get_arguments("capture_bed").get_value()
        qc_parser   = self.get_arguments("qc_parser").get_value()
        sample_name = self.get_arguments("sample_name").get_value()

        # Get output file
        qc_report = self.get_output("qc_report")

        # Generating command to parse bedtools coverage output
        cmd = "%s BedtoolsIntersect -i %s -s %s > %s !LOG2!" % (qc_parser, capture_bed, sample_name, qc_report)

        return cmd
