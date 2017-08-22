from Modules import Module

class SnpSiftAnnotate(Module):

    def __init__(self, module_id):
        super(SnpSiftAnnotate, self).__init__(module_id)

        self.input_keys     = ["vcf", "snpsift", "java", "dbsnp", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpsift",            is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("dbsnp",              is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=1)
        self.add_argument("mem",                is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(split_name=split_name, extension=".vcf")
        self.add_output(platform, "vcf", vcf)

    def define_command(self, platform):
        # Get input arguments
        vcf_in      = self.get_arguments("vcf").get_value()
        snpsift     = self.get_arguments("snpsift").get_value()
        java        = self.get_arguments("java").get_value()
        dbsnp       = self.get_arguments("dbsnp").get_value()
        mem         = self.get_arguments("mem").get_value()

        # Get output file
        vcf_out = self.get_output("vcf")

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, platform.get_workspace_dir("tmp"))

        # Generating SnpEff command
        cmd = "%s %s -jar %s annotate %s %s > %s !LOG3!" % (java, jvm_options, snpsift, dbsnp, vcf_in, vcf_out)
        return cmd