from Modules import Module

class CatRecodedVCF(Module):
    def __init__(self, module_id):
        super(CatRecodedVCF, self).__init__(module_id)

        self.input_keys     = ["recoded_vcf", "nr_cpus", "mem"]
        self.output_keys    = ["recoded_vcf"]

        #Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("recoded_vcf",        is_required=True)
        self.add_argument("cat_recoded_vcf",    is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=1)

    def define_output(self, platform, split_name=None):
        # Declare merged samtools depth output filename
        recoded_vcf_out = self.generate_unique_file_name(extension=".recoded.vcf.txt")
        self.add_output(platform, "recoded_vcf", recoded_vcf_out)

    def define_command(self, platform):
        cat_recode_vcf  = self.get_arguments("cat_recoded_vcf").get_value()
        recode_vcf_in   = self.get_arguments("recoded_vcf")
        recode_vcf_out  = self.get_output("recoded_vcf")

        # Generating command for concatenating multiple files together using unix Cat command
        cmd = "python %s -i %s -vvv --output %s !LOG2!" % (cat_recode_vcf, " ".join(recode_vcf_in), recode_vcf_out)
        return cmd