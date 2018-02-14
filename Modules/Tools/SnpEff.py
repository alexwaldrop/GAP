from Modules import Module

class SnpEff(Module):

    def __init__(self, module_id):
        super(SnpEff, self).__init__(module_id)

        self.input_keys     = ["vcf", "snpeff", "java", "snpeff_ref", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpeff_ref",         is_required=True)
        self.add_argument("snpeff",             is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=6)

    def define_output(self, platform, split_name=None):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(split_name=split_name, extension=".vcf")
        self.add_output(platform, "vcf", vcf)

    def define_command(self, platform):
        # Get input arguments
        vcf_in      = self.get_arguments("vcf").get_value()
        snpeff      = self.get_arguments("snpeff").get_value()
        java        = self.get_arguments("java").get_value()
        snpeff_ref  = self.get_arguments("snpeff_ref").get_value()
        mem         = self.get_arguments("mem").get_value()

        # Get output file
        vcf_out = self.get_output("vcf")

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, platform.get_workspace_dir("tmp"))

        # Generating SnpEff command
        cmd = "%s %s -jar %s %s %s > %s !LOG2!" % (java, jvm_options, snpeff, snpeff_ref, vcf_in, vcf_out)
        return cmd