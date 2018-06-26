import logging

from System.Datastore import GAPFile
from Modules import Module

class ConcatFastq(Module):
    # Module designed to concatentate one or more R1, R2 files from the same sample
    # An example would be if you'd resequenced the same sample and wanted to used all sequence data as if it were a single FASTQ
    # If > 1 read pair: concat to a single read pair
    # If 1 read pair: return original file name without doing anything
    def __init__(self, module_id, is_docker=False):
        super(ConcatFastq, self).__init__(module_id, is_docker)
        self.output_keys    = ["R1", "R2"]

    def define_input(self):
        self.add_argument("R1",         is_required=True)
        self.add_argument("R2")
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):

        # Declare R1 output name
        r1 = self.get_argument("R1")
        if not isinstance(r1, list):
            # Just pass the filename as is if no concatenation required (num R1 = 1)
            self.add_output("R1", r1)
        else:
            # Concatenate R1 files to new output
            extension = ".R1.fastq.gz" if r1[0].endswith(".gz") else "concat.R1.fastq"
            self.add_output("R1", self.generate_unique_file_name(extension=extension))

        # Declare R2 output name
        r2 = self.get_argument("R2")
        if not isinstance(r2, list):
            # Either R2 is single path or R2 is None
            self.add_output("R2", r2)
        else:
            extension = ".R2.fastq.gz" if r2[0].endswith(".gz") else "concat.R2.fastq"
            self.add_output("R2", self.generate_unique_file_name(extension=extension))

    def define_command(self):
        # Generate command for running Fastqc
        r1      = self.get_argument("R1")
        r2      = self.get_argument("R2")
        r1_out  = self.get_output("R1")
        r2_out  = self.get_output("R2")

        # Check to make sure r1 and r2 contain same number of files
        self.__check_input(r1, r2)

        cmd = None
        if r1 != r1_out:
            # Concat R1 if necessary
            cmd = "cat %s > %s !LOG2!" % (" ".join(r1), r1_out)

        if r2 != r2_out:
            # Concat R2 if necessary
            r2_cmd = "cat %s > %s !LOG2!" % (" ".join(r2), r2_out)
            # Join in the background so they run at the same time
            cmd = "%s & %s ; wait" % (cmd, r2_cmd)

        return cmd

    def __check_input(self, r1, r2):
        # Make sure each contains same number of fastq files
        error = False
        multi_r1    = isinstance(r1, list)
        multi_r2    = isinstance(r2, list)
        single_r1   = isinstance(r1, GAPFile) or isinstance(r1, basestring)
        single_r2   = isinstance(r2, GAPFile) or isinstance(r2, basestring)
        if multi_r1:
            if multi_r2 and len(r1) != len(r2):
                # Multiple R1, R2 but not the same in each
                error = True
                logging.error("ConcatFastq error! Input must contain same number of R1(%d) and R2(%d) fastq files!" % (len(r1), len(r2)))
            elif single_r2:
                # Multiple R1 only one R2
                error = True
                logging.error("ConcatFastq error! Input must contain same number of R1(%d) and R2(1) fastq files!" % len(r1))
        elif multi_r2 and single_r1:
            # One R1 multiple R2
            error = True
            logging.error("ConcatFastq error! Input must contain same number of R1(1) and R2(%d) fastq files!" % len(r2))
        if error:
            raise RuntimeError("Incorrect input to ConcatFastq!")


class RecodeVCF(Module):

    def __init__(self, module_id, is_docker=False):
        super(RecodeVCF, self).__init__(module_id, is_docker)
        self.output_keys    = ["recoded_vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("recode_vcf",         is_required=True,   is_resource=True)   # Path to RecodeVCF.py executable
        self.add_argument("min-call-depth",     is_required=True,   default_value=10)   # Minimum reads supporting an allele to call a GT
        self.add_argument("info-columns",       is_required=False)                      # Optional list of INFO column names to include in output
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        recoded_vcf = self.generate_unique_file_name(extension=".recoded.vcf.txt")
        self.add_output("recoded_vcf", recoded_vcf)

    def define_command(self):
        # Get input arguments
        vcf_in              = self.get_argument("vcf")
        recode_vcf_exec     = self.get_argument("recode_vcf")
        min_call_depth      = self.get_argument("min-call-depth")
        info_columns        = self.get_argument("info-columns")

        # Get final recoded VCF output file path
        recoded_vcf_out = self.get_output("recoded_vcf")

        # Generate base command
        if not self.is_docker:
            cmd = "sudo -H pip install -U pyvcf ; python %s --vcf %s --output %s --min-call-depth %s -vvv" % (recode_vcf_exec, vcf_in, recoded_vcf_out, min_call_depth)
        else:
            cmd = "python %s --vcf %s --output %s --min-call-depth %s -vvv" % (recode_vcf_exec, vcf_in, recoded_vcf_out, min_call_depth)

        # Optionally point to file specifying which vcf INFO fields to include in recoded output file
        if isinstance(info_columns, list):
            cmd += " --info-columns %s" % ",".join(info_columns)
        elif isinstance(info_columns, basestring):
            cmd += " --info-columns %s" % info_columns

        # Capture stderr
        cmd += " !LOG3!"

        # Return cmd
        return cmd


class SummarizeVCF(Module):

    def __init__(self, module_id, is_docker=False):
        super(SummarizeVCF, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_summary"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("summarize_vcf",      is_required=True,   is_resource=True)   # Path to SummarizeVCF.py executable
        self.add_argument("summary_type",       is_required=True,   default_value="Multisample")
        self.add_argument("max_records",        is_required=False,  default_value=None) # Number of variants to process (None = all variants)
        self.add_argument("max_depth",          is_required=False,  default_value=None) # Upper limit of depth histogram
        self.add_argument("max_indel_len",      is_required=False,  default_value=None) # Upper limit of indel length histogram
        self.add_argument("max_qual",           is_required=False,  default_value=None) # Upper limit of quality score histogram
        self.add_argument("num_afs_bins",       is_required=False,  default_value=None) # Number of histogram bins for alternate allele frequency (AAF) distribution
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_summary = self.generate_unique_file_name(extension=".summary.txt")
        self.add_output("vcf_summary", vcf_summary)

    def define_command(self):
        # Get input arguments
        vcf_in              = self.get_argument("vcf")
        summarize_vcf_exec  = self.get_argument("summarize_vcf")
        summary_type        = self.get_argument("summary_type")

        # Optional arguments
        max_records     = self.get_argument("max_records")
        max_depth       = self.get_argument("max_depth")
        max_indel_len   = self.get_argument("max_indel_len")
        max_qual        = self.get_argument("max_qual")
        num_afs_bins    = self.get_argument("num_afs_bins")

        # Get final recoded VCF output file path
        vcf_summary = self.get_output("vcf_summary")

        # Generate base command
        if not self.is_docker:
            cmd = "sudo -H pip install -U pyvcf ; python %s %s --vcf %s -vvv" % (summarize_vcf_exec, summary_type, vcf_in)
        else:
            cmd = "python %s %s --vcf %s -vvv" % (summarize_vcf_exec, summary_type, vcf_in)

        # Optionally point to file specifying which vcf INFO fields to include in recoded output file
        if max_records is not None:
            cmd += " --max-records %s" % max_records

        # Optionally specify upper limit of depth histogram
        if max_depth is not None:
            cmd += " --max-depth %s" % max_depth

        # Optionally specify upper limit of indel length histogram
        if max_indel_len is not None:
            cmd += " --max-indel-len %s" % max_indel_len

        # Optionallyk specify upper limit of quality score histogram
        if max_qual is not None:
            cmd += " --max-qual %s" % max_qual

        # Optionally specify number of bins for alternate allele frequency spectrum (AAFS)
        if num_afs_bins is not None:
            cmd += " --afs-bins %s" % num_afs_bins

        # Capture stderr and write stdout to output file
        cmd += " > %s !LOG2!" % vcf_summary

        # Return cmd
        return cmd


class ViralFilter(Module):
    def __init__(self, module_id, is_docker=False):
        super(ViralFilter, self).__init__(module_id, is_docker)
        self.output_keys = ["bam"]

    def define_input(self):
        self.add_argument("bam",                    is_required=True)
        self.add_argument("viral_filter",           is_resource=True, is_required=True)
        self.add_argument("nr_cpus",                is_required=True, default_value=1)
        self.add_argument("mem",                    is_required=True, default_value=4)
        self.add_argument("min_align_length",       is_required=False, default_value=40)
        self.add_argument("min_map_quality",        is_required=False, default_value=30)
        self.add_argument("only_properly_paired",   is_required=False, default_value=False)
        self.add_argument("max_window_length",      is_required=False, default_value=3)
        self.add_argument("max_window_freq",        is_required=False, default_value=0.6)

    def define_output(self):
        # Declare output bam filename
        output_bam = self.generate_unique_file_name(extension=".filtered.bam")
        self.add_output("bam", output_bam)

    def define_command(self):
        # Define command for running viral filter from a platform
        bam                 = self.get_argument("bam")
        viral_filter        = self.get_argument("viral_filter")
        min_align_length    = self.get_argument("min_align_length")
        min_map_quality     = self.get_argument("min_map_quality")
        only_properly_paired = self.get_argument("only_properly_paired")
        max_window_length   = self.get_argument("max_window_length")
        max_window_freq     = self.get_argument("max_window_freq")
        output_bam          = self.get_output("bam")

        # Generating filtering command
        if not self.is_docker:
            return "sudo -H pip install -U pysam; {0} {1} -v {2} -o {3} -l {4} -q {5} -w {6} -f {7}".format(
                viral_filter, "-p" if only_properly_paired else "", bam, output_bam, min_align_length,
                min_map_quality, max_window_length, max_window_freq)

        return "{0} {1} -v {2} -o {3} -l {4} -q {5} -w {6} -f {7}".format(
            viral_filter, "-p" if only_properly_paired else "", bam, output_bam, min_align_length,
            min_map_quality, max_window_length, max_window_freq)


class BGZip(Module):

    def __init__(self, module_id, is_docker=False):
        super(BGZip, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_gz"]

    def define_input(self):
        self.add_argument("vcf",        is_required=True)                       # Input VCF file
        self.add_argument("bgzip",      is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_in = self.get_argument("vcf")

        self.add_output("vcf_gz", "{0}.gz".format(vcf_in))

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        bgzip       = self.get_argument("bgzip")
        vcf_out     = self.get_output("vcf_gz")

        # Get final normalized VCF output file path
        cmd = "{0} {1} > {2} !LOG3!".format(bgzip, vcf_in, vcf_out)
        return cmd
