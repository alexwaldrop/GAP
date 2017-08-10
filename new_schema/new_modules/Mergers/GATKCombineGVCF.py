from Module import Module

class GATKCombineGVCF(Module):

    def __init__(self, module_id):
        super(GATKCombineGVCF, self).__init__(module_id)

        self.input_keys   = ["gvcf", "gvcf_idx", "gatk", "java", "nr_cpus", "mem"]
        self.output_keys  = ["gvcf", "gvcf_idx"]

    def define_input(self):
        self.add_argument("gvcf",               is_required=True)
        self.add_argument("gvcf_idx",           is_required=True)
        self.add_argument("gatk",               is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=12)

    def define_output(self, platform, split_name=None):
        # Declare merged GVCF output filename
        gvcf = self.generate_unique_file_name(extension=".g.vcf")
        self.add_output(platform, "gvcf", gvcf)
        # Declare GVCF index output filename
        gvcf_idx = self.generate_unique_file_name(extension=".g.vcf.idx")
        self.add_output(platform, "gvcf_idx", gvcf_idx)

    def define_command(self, platform):

        # Obtaining the arguments
        gvcf_list   = self.get_arguments("gvcf").get_value()
        gatk        = self.get_arguments("gatk").get_value()
        java        = self.get_arguments("java").get_value()
        ref         = self.get_arguments("ref").get_value()
        mem         = self.get_arguments("mem").get_value()
        gvcf_out    = self.get_output("gvcf")

        # Generating JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, platform.get_workspace_dir("tmp"))

        # Generating the combine options
        opts = list()
        opts.append("-o %s" % gvcf_out)
        opts.append("-R %s" % ref)
        for gvcf_input in gvcf_list:
            opts.append("-V %s" % gvcf_input)

        # Generating the combine command
        cmd = "%s %s -jar %s -T CombineGVCFs %s !LOG3!" % (java,
                                                                jvm_options,
                                                                gatk,
                                                                " ".join(opts))
        return cmd