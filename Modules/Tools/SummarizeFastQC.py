import os

from Modules import Module

class SummarizeFastQC(Module):

    def __init__(self, module_id):
        super(SummarizeFastQC, self).__init__(module_id)

        self.input_keys     = ["R1_fastqc", "R2_fastqc", "qc_parser", "nr_cpus", "mem"]
        self.output_keys    = ["summary_file"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("R1_fastqc",  is_required=True)
        self.add_argument("R2_fastqc",  is_required=False)
        self.add_argument("qc_parser",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)
        self.add_argument("fastq_type")

    def define_output(self, platform, split_name=None):
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".fastqc.summary.txt")
        self.add_output(platform, "summary_file", summary_file)

    def define_command(self, platform):
        # Get options from kwargs
        r1_fastqc_dir       = self.get_arguments("R1_fastqc").get_value()
        r2_fastqc_dir       = self.get_arguments("R2_fastqc").get_value()
        fastq_type          = self.get_arguments("fastq_type").get_value()
        qc_parser           = self.get_arguments("qc_parser").get_value()
        summary_file        = self.get_output("summary_file")

        # Get command for parsing R1 fastqc output
        r1_column_header = "R1_%s" % fastq_type if fastq_type is not None else None
        r1_parse_cmd, r1_output = self.__get_one_fastqc_cmd(r1_fastqc_dir,
                                                            qc_parser,
                                                            column_header_suffix=r1_column_header)

        # cmd to summarize only R1 fastqc output and cat to correct output filename
        cmd = "%s ; cat %s > %s" % (r1_parse_cmd, r1_output, summary_file)

        # Conditionally append command for parsing R2 fastqc output
        if r2_fastqc_dir is not None:
            r2_column_header = "R2_%s" % fastq_type if fastq_type is not None else None
            r2_parse_cmd, r2_output = self.__get_one_fastqc_cmd(r2_fastqc_dir,
                                                                qc_parser,
                                                                column_header_suffix=r2_column_header)

            # cmd to summarize R1 and R2 and paste together into a single output file
            cmd = "%s ; %s ; paste %s %s > %s !LOG2!" % (r1_parse_cmd, r2_parse_cmd,
                                                         r1_output, r2_output, summary_file)
        return cmd

    @staticmethod
    def __get_one_fastqc_cmd(fastqc_dir, qc_parser, column_header_suffix=None):
        # Get command for summarizing output from fastqc

        # Get input filename
        fastqc_summary_file = os.path.join(fastqc_dir, "fastqc_data.txt")

        # Get output filename
        output = "%s.fastqcsummary.txt" % fastqc_summary_file.split("_fastqc")[0]

        cmd = "%s fastqc -i %s" % (qc_parser, fastqc_summary_file)

        # Optionally add a string to column headers in output table
        cmd += " -p %s" % column_header_suffix if column_header_suffix is not None else ""

        # Write to output file and log errors
        cmd += " > %s !LOG2!" % output

        return cmd, output