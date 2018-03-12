from Modules import Module

class RecodeVCF(Module):

    def __init__(self, module_id):
        super(RecodeVCF, self).__init__(module_id)

        self.input_keys     = ["vcf", "recode_vcf", "nr_cpus", "mem"]
        self.output_keys    = ["recoded_vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("recode_vcf",         is_required=True,   is_resource=True)   # Path to RecodeVCF.py executable
        self.add_argument("min-call-depth",     is_required=True,   default_value=10)   # Minimum reads supporting an allele to call a GT
        self.add_argument("info-columns",       is_required=False)                      # Optional list of INFO column names to include in output
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=2)

    def define_output(self, platform, split_name=None):
        # Declare recoded VCF output filename
        recoded_vcf = self.generate_unique_file_name(split_name=split_name, extension=".recoded.vcf.txt")
        self.add_output(platform, "recoded_vcf", recoded_vcf)

    def define_command(self, platform):
        # Get input arguments
        vcf_in              = self.get_arguments("vcf").get_value()
        recode_vcf_exec     = self.get_arguments("recode_vcf").get_value()
        min_call_depth      = self.get_arguments("min-call-depth").get_value()
        info_columns        = self.get_arguments("info-columns").get_value()

        # Get final recoded VCF output file path
        recoded_vcf_out = self.get_output("recoded_vcf")

        # Generate base command
        cmd = "sudo -H pip install -U pyvcf ; python %s --vcf %s --output %s --min-call-depth %s -vvv" % (recode_vcf_exec, vcf_in, recoded_vcf_out, min_call_depth)

        # Optionally point to file specifying which vcf INFO fields to include in recoded output file
        if isinstance(info_columns, list):
            cmd += " --info-columns %s" % ",".join(info_columns)
        elif isinstance(info_columns, basestring):
            cmd += " --info-columns %s" % info_columns

        # Capture stderr
        cmd += " !LOG3!"

        # Return cmd
        return cmd
