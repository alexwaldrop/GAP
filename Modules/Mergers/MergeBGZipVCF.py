from Modules import Module

class MergeBGZipVCF(Module):

    def __init__(self, module_id):
        super(MergeBGZipVCF, self).__init__(module_id)

        self.input_keys     = ["bcftools", "vcf_gz", "vcf_csi", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("vcf_gz",     is_required=True)
        self.add_argument("vcf_csi",    is_required=True)
        self.add_argument("bcftools",   is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=4)
        self.add_argument("mem",        is_required=True,   default_value="nr_cpus*4")

    def define_output(self, platform, split_name=None):
        # Declare recoded VCF output filename
        vcf_out = self.generate_unique_file_name(extension=".vcf")
        self.add_output(platform, "vcf", vcf_out)

    def define_command(self, platform):
        # Get input arguments
        vcf_gz      = self.get_arguments("vcf_gz").get_value()
        bcftools    = self.get_arguments("bcftools").get_value()
        vcf_out     = self.get_output("vcf")
        # Get final normalized VCF output file path

        cmd = "%s merge %s > %s !LOG2!" % (bcftools, " ".join(vcf_gz), vcf_out)
        return cmd