import os

from Modules import Module

class _QCParser(Module):

    def __init__(self, module_id, is_docker=False):
        super(_QCParser, self).__init__(module_id, is_docker)
        self.output_keys    = ["qc_report"]

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("note",           is_required=False,  default_value=None)
        self.add_argument("qc_parser",      is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self):
        summary_file = self.generate_unique_file_name(extension=".qc_report.json")
        self.add_output("qc_report", summary_file)


class FastQC(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(FastQC, self).__init__(module_id, is_docker)

    def define_input(self):
        super(FastQC, self).define_input()
        self.add_argument("R1_fastqc",      is_required=True)
        self.add_argument("R2_fastqc",      is_required=False)

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


class PicardInsertMetrics(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(PicardInsertMetrics, self).__init__(module_id, is_docker)

    def define_input(self):
        super(PicardInsertMetrics, self).define_input()
        self.add_argument("insert_size_report", is_required=True)

    def define_command(self):
        # Get options from kwargs
        input_file      = self.get_argument("insert_size_report")
        qc_parser       = self.get_argument("qc_parser")
        sample_name     = self.get_argument("sample_name")
        parser_note     = self.get_argument("note")
        qc_report       = self.get_output("qc_report")

        # Generate PicardInsertSize parser basecommand
        cmd = "%s PicardInsertSize -i %s -s %s" % (qc_parser, input_file, sample_name)

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Output qc_report to file
        cmd += " > %s !LOG2!" % qc_report
        return cmd


class SamtoolsDepth(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(SamtoolsDepth, self).__init__(module_id, is_docker)

    def define_input(self):
        self.add_argument("samtools_depth",     is_required=True)
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("note",               is_required=False, default_value=None)
        self.add_argument("qc_parser",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=1)
        self.add_argument("mem",                is_required=True, default_value=12)
        self.add_argument("depth_cutoffs",      is_required=True, default_value=[1,5,10,15,25,50,100])

    def define_command(self):

        # Get options from kwargs
        input_file      = self.get_argument("samtools_depth")
        qc_parser       = self.get_argument("qc_parser")
        sample_name     = self.get_argument("sample_name")
        cutoffs         = self.get_argument("depth_cutoffs")
        parser_note     = self.get_argument("note")
        qc_report       = self.get_output("qc_report")

        # Generating command to parse samtools depth output
        cmd = "%s SamtoolsDepth -i %s -s %s " % (qc_parser, input_file, sample_name)

        # Add options for coverage depth cutoffs to report
        for cutoff in cutoffs:
            cutoff = int(cutoff)
            cmd += " --ct %d" % cutoff

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Write output to summary file
        cmd += " > %s !LOG2!" % qc_report
        return cmd


class SamtoolsFlagstat(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(SamtoolsFlagstat, self).__init__(module_id, is_docker)

    def define_input(self):
        super(SamtoolsFlagstat, self).define_input()
        self.add_argument("flagstat",   is_required=True)

    def define_command(self):
        # Get options from kwargs
        input_file      = self.get_argument("flagstat")
        qc_parser       = self.get_argument("qc_parser")
        sample_name     = self.get_argument("sample_name")
        parser_note     = self.get_argument("note")
        qc_report       = self.get_output("qc_report")

        # Generate base command
        cmd = "%s SamtoolsFlagstat -i %s -s %s" % (qc_parser, input_file, sample_name)

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Output qc_report to file
        cmd += " > %s !LOG2!" % qc_report
        return cmd


class SamtoolsIdxstats(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(SamtoolsIdxstats, self).__init__(module_id, is_docker)

    def define_input(self):
        super(SamtoolsIdxstats, self).define_input()
        self.add_argument("idxstats", is_required=True)

    def define_command(self):
        # Get options from kwargs
        input_file      = self.get_argument("idxstats")
        qc_parser       = self.get_argument("qc_parser")
        sample_name     = self.get_argument("sample_name")
        parser_note     = self.get_argument("note")
        qc_report       = self.get_output("qc_report")

        # Generate base command
        cmd = "%s SamtoolsIdxstats -i %s -s %s" % (qc_parser, input_file, sample_name)

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Output qc_report to file
        cmd += " > %s !LOG2!" % qc_report
        return cmd


class Trimmomatic(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(Trimmomatic, self).__init__(module_id, is_docker)

    def define_input(self):
        super(Trimmomatic, self).define_input()
        self.add_argument("trim_report", is_required=True)

    def define_command(self):
        # Get options from kwargs
        input_file      = self.get_argument("trim_report")
        qc_parser       = self.get_argument("qc_parser")
        sample_name     = self.get_argument("sample_name")
        parser_note     = self.get_argument("note")
        qc_report       = self.get_output("qc_report")

        # Create base command
        cmd = "%s Trimmomatic -i %s -s %s" % (qc_parser, input_file, sample_name)

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Output qc_report to file
        cmd += " > %s !LOG2!" % qc_report
        return cmd


class PrintTable(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(PrintTable, self).__init__(module_id, is_docker)

    def define_input(self):
        self.add_argument("qc_report",      is_required=True)
        self.add_argument("qc_parser",      is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=1)
        self.add_argument("col_order")
        self.add_argument("alt_colnames")

    def define_output(self):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(extension=".qc_report.table.txt")
        self.add_output("qc_table", summary_file)

    def define_command(self):
        # Get options from kwargs
        input_file      = self.get_argument("qc_report")
        qc_parser       = self.get_argument("qc_parser")
        col_order       = self.get_argument("col_order")
        alt_colnames    = self.get_argument("alt_colnames")
        qc_table        = self.get_output("qc_table")

        # Create base command for PrintTable
        cmd = "%s PrintTable -i %s" % (qc_parser, input_file)

        # Add special arguments if necessary
        if col_order is not None:
            cmd += " --col-order %s" % " ".join(col_order)

        if alt_colnames is not None:
            cmd += " --alt-colnames %s" % " ".join(alt_colnames)

        # Direct output to output file
        cmd += " > %s !LOG2!" % qc_table
        return cmd


class MosdepthDist(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(MosdepthDist, self).__init__(module_id, is_docker)

    def define_input(self):
        super(MosdepthDist, self).define_input()
        self.add_argument("mosdepth_dist", is_required=True)

    def define_command(self):
        # Get options from kwargs
        input_file      = self.get_argument("mosdepth_dist")
        qc_parser       = self.get_argument("qc_parser")
        sample_name     = self.get_argument("sample_name")
        parser_note     = self.get_argument("note")
        qc_report       = self.get_output("qc_report")

        # Generate base command
        cmd = "%s MosdepthDist -i %s -s %s" % (qc_parser, input_file, sample_name)

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Output qc_report to file
        cmd += " > %s !LOG2!" % qc_report
        return cmd


class GATKCollectReadCount(_QCParser):

    def __init__(self, module_id, is_docker=False):
        super(GATKCollectReadCount, self).__init__(module_id, is_docker)

    def define_input(self):
        super(GATKCollectReadCount, self).define_input()
        self.add_argument("read_count_out", is_required=True)

    def define_command(self):
        # Get options from kwargs
        input_file      = self.get_argument("read_count_out")
        qc_parser       = self.get_argument("qc_parser")
        sample_name     = self.get_argument("sample_name")
        parser_note     = self.get_argument("note")
        qc_report       = self.get_output("qc_report")

        # Generate base command
        cmd = "%s GATKCollectReadCount -i %s -s %s" % (qc_parser, input_file, sample_name)

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Output qc_report to file
        cmd += " > %s !LOG2!" % qc_report
        return cmd
