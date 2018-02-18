from Modules import Module

class GATKCombineGVCF(Module):

    def __init__(self, module_id):
        super(GATKCombineGVCF, self).__init__(module_id)

        self.input_keys   = ["gvcf", "gvcf_idx", "gatk", "java", "nr_cpus", "mem", "location", "excluded_location"]
        self.output_keys  = ["gvcf", "gvcf_idx"]

        self.quick_command = True

    def define_input(self):
        self.add_argument("gvcf",               is_required=True)
        self.add_argument("gvcf_idx",           is_required=True)
        self.add_argument("gatk",               is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=13)
        self.add_argument("location")
        self.add_argument("excluded_location")

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
        L           = self.get_arguments("location").get_value()
        XL          = self.get_arguments("excluded_location").get_value()
        gvcf_out    = self.get_output("gvcf")

        # Generating JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, platform.get_workspace_dir("tmp"))

        # Generating the combine options
        opts = list()
        opts.append("-o %s" % gvcf_out)
        opts.append("-R %s" % ref)
        opts.append("-U ALLOW_SEQ_DICT_INCOMPATIBILITY") # Option that allows dictionary incompatibility
        for gvcf_input in gvcf_list:
            opts.append("-V %s" % gvcf_input)

        # Limit the locations to be processes
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    if included != "unmapped":
                        opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating the combine command
        cmd = "%s %s -jar %s -T CombineGVCFs %s !LOG3!" % (java,
                                                                jvm_options,
                                                                gatk,
                                                                " ".join(opts))
        return cmd