from Modules import Module

class BGZipVCF(Module):

    def __init__(self, module_id):
        super(BGZipVCF, self).__init__(module_id)

        self.input_keys     = ["bcftools", "vcf", "nr_cpus", "mem"]
        self.output_keys    = ["vcf_gz", "vcf_csi"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("bcftools",           is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=2)

    def define_output(self, platform, split_name=None):
        # Declare recoded VCF output filename
        vcf_in = self.get_arguments("vcf").get_value()
        self.add_output(platform, "vcf_gz", vcf_in+".gz")
        self.add_output(platform, "vcf_csi", vcf_in+".gz.csi")

    def define_command(self, platform):
        # Get input arguments
        vcf_in      = self.get_arguments("vcf").get_value()
        bcftools    = self.get_arguments("bcftools").get_value()
        vcf_out     = self.get_output("vcf_gz")
        # Get final normalized VCF output file path

        cmd = "bgzip %s !LOG2!; %s index -f %s !LOG2!" % (vcf_in, bcftools, vcf_out)
        return cmd