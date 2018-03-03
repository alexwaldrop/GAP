from Modules import Module

class CatVCFSummary(Module):
    def __init__(self, module_id):
        super(CatVCFSummary, self).__init__(module_id)

        self.input_keys     = ["vcf_summary", "nr_cpus", "mem"]
        self.output_keys    = ["vcf_summary"]

        #Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("vcf_summary",        is_required=True)
        self.add_argument("cat_vcf_summary",    is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=1)

    def define_output(self, platform, split_name=None):
        # Declare merged samtools depth output filename
        vcf_summary_out = self.generate_unique_file_name(extension=".summary.txt")
        self.add_output(platform, "vcf_summary", vcf_summary_out)

    def define_command(self, platform):
        cat_vcf_summary  = self.get_arguments("cat_vcf_summary").get_value()
        vcf_summary_in   = self.get_arguments("vcf_summary").get_value()
        vcf_summary_out  = self.get_output("vcf_summary")

        # Generating command for concatenating multiple files together using unix Cat command
        cmd = "sudo pip install -U pyvcf ; python %s -i %s -vvv > %s !LOG2!" % (cat_vcf_summary, " ".join(vcf_summary_in), vcf_summary_out)
        return cmd