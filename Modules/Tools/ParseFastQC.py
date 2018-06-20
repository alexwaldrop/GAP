import os

from Modules import Module

class ParseFastQC(Module):

    def __init__(self, module_id, is_docker=False):
        super(ParseFastQC, self).__init__(module_id, is_docker)
        self.output_keys    = ["qc_report"]

    def define_input(self):
        self.add_argument("R1_fastqc",      is_required=True)
        self.add_argument("R2_fastqc",      is_required=False)
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("note",           is_required=False,  default_value=None)
        self.add_argument("qc_parser",      is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self):
        summary_file = self.generate_unique_file_name(extension=".fastqc.qc_report.json")
        self.add_output("qc_report", summary_file)

    def define_command(self):
        # Get options from kwargs
        r1_fastqc_dir   = self.get_argument("R1_fastqc")
        r2_fastqc_dir   = self.get_argument("R2_fastqc")
        qc_parser       = self.get_argument("qc_parser")
        sample_name     = self.get_argument("sample_name")
        parser_note     = self.get_argument("note")
        qc_report       = self.get_output("qc_report")

        # Get command for parsing R1 fastqc output
        r1_parse_cmd, r1_output = self.__get_one_fastqc_cmd(r1_fastqc_dir, qc_parser, sample_name, parser_note)

        if r2_fastqc_dir is None:
            # Case: No R1 provided
            cmd = "%s ; cat %s > %s" % (r1_parse_cmd, r1_output, qc_report)

        else:
            # Case: R2 provided

            # Generate QCReport for R2 fastqc
            r2_parse_cmd, r2_output = self.__get_one_fastqc_cmd(r2_fastqc_dir, qc_parser, sample_name, parser_note)

            # Rbind R1 and R2 QCReports into a single report
            rbind_cmd = "%s Cbind -i %s %s > %s !LOG2!" % (qc_parser, r1_output, r2_output, qc_report)

            # cmd to summarize R1 and R2 and paste together into a single output file
            cmd = "%s ; %s ; %s" % (r1_parse_cmd, r2_parse_cmd, rbind_cmd)

        return cmd

    @staticmethod
    def __get_one_fastqc_cmd(fastqc_dir, qc_parser, sample_name, parser_note=None):
        # Get command for summarizing output from fastqc

        # Get input filename
        fastqc_summary_file = os.path.join(fastqc_dir, "fastqc_data.txt")

        # Get output filename
        output = "%s.fastqcsummary.txt" % fastqc_summary_file.split("_fastqc")[0]

        # Generate base command
        cmd = "%s FastQC -i %s -s %s" % (qc_parser, fastqc_summary_file, sample_name)

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Output qc_report to file
        cmd += " > %s !LOG2!" % output
        return cmd, output