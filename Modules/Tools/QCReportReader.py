from Modules import Module

class _QCReportReader(Module):

    def __init__(self, module_id, is_docker=False):
        super(_QCReportReader, self).__init__(module_id, is_docker)

    def define_input(self):
        self.add_argument("qc_report",      is_required=True)
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self):
        pass

    def define_command(self):
        # Spit qc report to stdout for parsing
        qc_report       = self.get_argument("qc_report")
        return "cat %s !LOG2!" % qc_report

    @staticmethod
    def __parse_qc_report(out):
        # Return QCReport parsed from stdout
        pass

class GetNumReads(_QCReportReader):

    def __init__(self, module_id, is_docker=False):
        super(GetNumReads, self).__init__(module_id, is_docker)
        self.output_keys = ["nr_reads"]

    def define_input(self):
        super(GetNumReads, self).define_input()
        self.add_argument("filter_by_note")

    def define_output(self):
        self.add_output("nr_reads", 0, is_path=False)

    def process_cmd_output(self, out, err):
        # Parse numreads from FastQC sections of QCReport
        qc_report = self.__parse_qc_report(out)

        sample_name     = self.get_argument("sample_name")
        filter_by_note  = self.get_argument("filter_by_note")
        num_reads       = sum(qc_report.fetch_values(sample_name,
                                                     module="FastQC",
                                                     key="Total_Reads",
                                                     note=filter_by_note))
        self.set_output("nr_reads", num_reads)
