from Modules import Module

class QCReportCbind(Module):

    def __init__(self, module_id):
        super(QCReportCbind, self).__init__(module_id)

        self.input_keys     = ["qc_parser", "qc_report", "nr_cpus", "mem"]
        self.output_keys    = ["qc_report"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("qc_report",  is_required=True)
        self.add_argument("qc_parser",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension="rbind.qc_report.txt")
        self.add_output(platform, "qc_report", summary_file)

    def define_command(self, platform):
        # Get options from kwargs
        input_files     = self.get_arguments("qc_report").get_value()
        qc_parser       = self.get_arguments("qc_parser").get_value()
        qc_report       = self.get_output("qc_report")
        return "%s Cbind -i %s > %s !LOG2!" % (qc_parser, " ".join(input_files), qc_report)