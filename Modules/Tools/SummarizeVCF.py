from Modules import Module

class SummarizeVCF(Module):

    def __init__(self, module_id):
        super(SummarizeVCF, self).__init__(module_id)

        self.input_keys     = ["vcf", "summarize_vcf", "nr_cpus", "mem"]
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

    def define_output(self, platform, split_name=None):
        # Declare recoded VCF output filename
        vcf_summary = self.generate_unique_file_name(split_name=split_name, extension=".summary.txt")
        self.add_output(platform, "vcf_summary", vcf_summary)

    def define_command(self, platform):
        # Get input arguments
        vcf_in              = self.get_arguments("vcf").get_value()
        summarize_vcf_exec  = self.get_arguments("summarize_vcf").get_value()
        summary_type        = self.get_arguments("summary_type").get_value()

        # Optional arguments
        max_records     = self.get_arguments("max_records").get_value()
        max_depth       = self.get_arguments("max_depth").get_value()
        max_indel_len   = self.get_arguments("max_indel_len").get_value()
        max_qual        = self.get_arguments("max_qual").get_value()
        num_afs_bins    = self.get_arguments("num_afs_bins").get_value()


        # Get final recoded VCF output file path
        vcf_summary = self.get_output("vcf_summary")

        # Generate base command
        cmd = "sudo pip install -U pyvcf ; python %s %s --vcf %s -vvv" % (summarize_vcf_exec, summary_type, vcf_in)

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
