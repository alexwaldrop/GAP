from Modules import Module

class BCFToolsNorm(Module):

    def __init__(self, module_id):
        super(BCFToolsNorm, self).__init__(module_id)

        self.input_keys     = ["bcftools", "vcf", "ref", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("bcftools",           is_required=True,   is_resource=True)
        self.add_argument("ref",                is_required=True,   is_resource=True)   # Path to Fasta formatted genome reference
        self.add_argument("split_multiallelic", is_required=True,   default_value=True) # Whether to split multiallelic sites into mutliple sites
        self.add_argument("nr_cpus",            is_required=True,   default_value=4)
        self.add_argument("mem",                is_required=True,   default_value="nr_cpus*4")

    def define_output(self, platform, split_name=None):
        # Declare recoded VCF output filename
        normalized_vcf = self.generate_unique_file_name(split_name=split_name, extension=".normalized.vcf")
        self.add_output(platform, "vcf", normalized_vcf)

    def define_command(self, platform):
        # Get input arguments
        vcf_in              = self.get_arguments("vcf").get_value()
        bcftools            = self.get_arguments("bcftools").get_value()
        ref                 = self.get_arguments("ref").get_value()
        split_multiallelic  = self.get_arguments("split_multiallelic").get_value()

        # Get final normalized VCF output file path
        vcf_out = self.get_output("vcf")

        cmd = "%s norm" % bcftools
        if split_multiallelic:
            # Optionally specify to split multiallelic into two lines
            cmd += " -m-both"
        cmd += " -f %s -o %s %s" % (ref, vcf_out, vcf_in)

        # Capture stderr
        cmd += " !LOG3!"

        # Return cmd
        return cmd
