from Modules import Merger, PseudoMerger

class Rbind(Merger):

    def __init__(self, module_id, is_docker=False):
        super(Rbind, self).__init__(module_id, is_docker)
        self.output_keys    = ["qc_report"]

    def define_input(self):
        self.add_argument("qc_report",  is_required=True)
        self.add_argument("qc_parser",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(extension="rbind.qc_report.json")
        self.add_output("qc_report", summary_file)

    def define_command(self):
        # Get options from kwargs
        input_files     = self.get_argument("qc_report")
        qc_parser       = self.get_argument("qc_parser")
        qc_report       = self.get_output("qc_report")
        if isinstance(input_files, list):
            return "%s Rbind -i %s > %s !LOG2!" % (qc_parser, " ".join(input_files), qc_report)
        else:
            return "%s Rbind -i %s > %s !LOG2!" % (qc_parser, input_files, qc_report)


class Cbind(PseudoMerger):

    def __init__(self, module_id, is_docker=False):
        super(Cbind, self).__init__(module_id, is_docker)
        self.output_keys    = ["qc_report"]

    def define_input(self):
        self.add_argument("qc_report",  is_required=True)
        self.add_argument("qc_parser",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(extension="cbind.qc_report.json")
        self.add_output("qc_report", summary_file)

    def define_command(self):
        # Get options from kwargs
        input_files     = self.get_argument("qc_report")
        qc_parser       = self.get_argument("qc_parser")
        qc_report       = self.get_output("qc_report")
        if isinstance(input_files, list):
            return "%s Cbind -i %s > %s !LOG2!" % (qc_parser, " ".join(input_files), qc_report)
        else:
            return "%s Cbind -i %s > %s !LOG2!" % (qc_parser, input_files, qc_report)