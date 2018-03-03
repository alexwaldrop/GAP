from Modules import Module

class IndexVCF(Module):

    def __init__(self, module_id):
        super(IndexVCF, self).__init__(module_id)

        self.input_keys   = ["vcf", "gatk", "java", "ref", "nr_cpus", "mem"]
        self.output_keys  = ["vcf", "vcf_idx"]

    def define_input(self):
        self.add_argument("vcf",               is_required=True)
        self.add_argument("gatk",               is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=13)

    def define_output(self, platform, split_name=None):
        # Declare merged GVCF output filename
        vcf = self.generate_unique_file_name(split_name=split_name, extension=".vcf")
        self.add_output(platform, "vcf", vcf)
        # Declare GVCF index output filename
        vcf_idx = vcf + ".idx"
        self.add_output(platform, "vcf_idx", vcf_idx)

    def define_command(self, platform):
        # Obtaining the arguments
        vcf_in   = self.get_arguments("vcf").get_value()
        gatk        = self.get_arguments("gatk").get_value()
        java        = self.get_arguments("java").get_value()
        ref         = self.get_arguments("ref").get_value()
        mem         = self.get_arguments("mem").get_value()
        vcf_out    = self.get_output("vcf")

        # Generating JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 9 / 10, platform.get_workspace_dir("tmp"))

        # Generating the CatVariants options
        opts = list()
        opts.append("-out %s" % vcf_out)
        opts.append("-R %s" % ref)
        opts.append("-V %s" % vcf_in)

        # Generating the combine command
        cmd = "%s %s -cp %s org.broadinstitute.gatk.tools.CatVariants %s !LOG3!" % (java,
                                                                                    jvm_options,
                                                                                    gatk,
                                                                                    " ".join(opts))
        return cmd
