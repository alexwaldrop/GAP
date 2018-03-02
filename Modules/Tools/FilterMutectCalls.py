from Modules import Module

class FilterMutectCalls(Module):

    def __init__(self, module_id):
        super(FilterMutectCalls, self).__init__(module_id)

        self.input_keys     = ["vcf", "java", "gatk", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("vcf",        is_required=True)
        self.add_argument("java",       is_required=True,   is_resource=True)
        self.add_argument("gatk",       is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=1)
        self.add_argument("mem",        is_required=True,   default_value=2)

    def define_output(self, platform, split_name=None):
        # Declare recoded VCF output filename
        vcf_out = self.generate_unique_file_name(split_name=split_name, extension=".vcf")
        self.add_output(platform, "vcf", vcf_out)

    def define_command(self, platform):
        # Get input arguments
        vcf_in  = self.get_arguments("vcf").get_value()
        java    = self.get_arguments("java").get_value()
        gatk    = self.get_arguments("gatk").get_value()
        vcf_out = self.get_output("vcf")

        # Generating command for base recalibration
        cmd = "%s -jar %s FilterMutectCalls -V %s -O %s !LOG3!" % (java, gatk, vcf_in, vcf_out)
        return cmd
