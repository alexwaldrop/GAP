from Modules import Module

class VCFMerger(Module):

    def __init__(self, module_id):
        super(VCFMerger, self).__init__(module_id)

        self.input_keys     = ["vcf", "snpsift", "java", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpsift",            is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=8)
        self.add_argument("mem",                is_required=True, default_value=40)

    def define_output(self, platform, split_name=None):
        # Declare name of merged VCF output file
        vcf_out = self.generate_unique_file_name(extension=".vcf")
        self.add_output(platform, "vcf", vcf_out)

    def define_command(self, platform):
        # Get input arguments
        vcf_list    = self.get_arguments("vcf").get_value()
        snpsift     = self.get_arguments("snpsift").get_value()
        java        = self.get_arguments("java").get_value()
        mem         = self.get_arguments("mem").get_value()
        vcf_out     = self.get_output("vcf")

        # Generating JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 9 / 10, platform.get_workspace_dir("tmp"))

        # Generating SnpEff command
        cmd = "%s %s -jar %s sort %s > %s !LOG2!" % (java, jvm_options, snpsift, " ".join(vcf_list), vcf_out)
        return cmd