from Modules import Module

class BCFToolsNorm(Module):

    def __init__(self, module_id, is_docker = False):
        super(BCFToolsNorm, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("bcftools",           is_required=True,   is_resource=True)
        self.add_argument("ref",                is_required=True,   is_resource=True)   # Path to Fasta formatted genome reference
        self.add_argument("split_multiallelic", is_required=True,   default_value=True) # Whether to split multiallelic sites into mutliple sites
        self.add_argument("nr_cpus",            is_required=True,   default_value=4)
        self.add_argument("mem",                is_required=True,   default_value="nr_cpus*4")

    def define_output(self):
        # Declare recoded VCF output filename
        normalized_vcf = self.generate_unique_file_name(extension=".normalized.vcf")
        self.add_output("vcf", normalized_vcf)

    def define_command(self):
        # Get input arguments
        vcf_in              = self.get_argument("vcf")
        bcftools            = self.get_argument("bcftools")
        ref                 = self.get_argument("ref")
        split_multiallelic  = self.get_argument("split_multiallelic")

        # Get final normalized VCF output file path
        vcf_out = self.get_output("vcf")

        cmd = "{0} norm".format(bcftools)
        if split_multiallelic:
            # Optionally specify to split multiallelic into two lines
            cmd += " -m-both"
        cmd += " -f {0} -o {1} {2}".format(ref, vcf_out, vcf_in)

        # Capture stderr
        cmd += " !LOG3!"

        # Return cmd
        return cmd
