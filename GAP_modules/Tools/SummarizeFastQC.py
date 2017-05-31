from GAP_interfaces import Tool
import os

__main_class__ = "SummarizeFastQC"

class SummarizeFastQC(Tool):

    def __init__(self, platform, tool_id):
        super(SummarizeFastQC, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = 1
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["R1_fastqc", "R2_fastqc"]
        self.output_keys    = ["summary_file"]

        self.req_tools      = ["qc_parser"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Get options from kwargs
        r1_fastqc_dir           = kwargs.get("R1_fastqc",               None)
        r2_fastqc_dir           = kwargs.get("R2_fastqc",               None)
        is_fastq_trimmed        = kwargs.get("is_fastq_trimmed",        False)
        omit_fastq_name         = kwargs.get("omit_fastq_name",         False)
        include_header_suffix   = kwargs.get("include_header_suffix",   True)
        qc_parser               = kwargs.get("qc_parser",               self.tools["qc_parser"])

        # Make header suffixes if necessary
        fastq_type = "Trimmed" if is_fastq_trimmed else "Raw"
        r1_column_header = "R1_%s" % fastq_type if include_header_suffix else None
        r2_column_header = "R2_%s" % fastq_type if include_header_suffix else None

        # Get commands for parsing each fastqc results file
        r1_parse_cmd, r1_output = self.get_one_fastqc_cmd(r1_fastqc_dir,
                                                          qc_parser,
                                                          omit_fastq_name,
                                                          column_header_suffix=r1_column_header)

        r2_parse_cmd, r2_output = self.get_one_fastqc_cmd(r2_fastqc_dir,
                                                          qc_parser,
                                                          omit_fastq_name,
                                                          column_header_suffix=r2_column_header)

        # final command
        cmd = "%s ; %s ; paste %s %s > %s !LOG2!" \
              % (r1_parse_cmd, r2_parse_cmd, r1_output, r2_output, self.output["summary_file"])

        return cmd

    def get_one_fastqc_cmd(self, fastqc_dir, qc_parser, omit_fastq_name, column_header_suffix=None):
        # Get command for summarizing output from fastqc

        # Get input filename
        fastqc_summary_file = os.path.join(fastqc_dir, "fastqc_data.txt")

        # Get output filename
        output = "%s.fastqcsummary.txt" % fastqc_summary_file.split("_fastqc")[0]

        cmd = "%s fastqc -i %s" % (qc_parser, fastqc_summary_file)

        # Optionally add a string to column headers in output table
        cmd += " -p %s" % column_header_suffix if column_header_suffix is not None else ""

        # Optionally add flag to not include name of fastq file in output table
        cmd += " --omitfilename" if omit_fastq_name else ""

        # Write to output file and log errors
        cmd += " > %s !LOG2!" % output

        return cmd, output

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("summary_file", "fastqc.summary.txt")

