from Modules import Module

class GATKHaplotypeCaller(Module):

    def __init__(self, module_id):
        super(GATKHaplotypeCaller, self).__init__(module_id)

        self.input_keys = ["bam", "bam_idx", "BQSR_report", "location",
                           "excluded_location", "gatk", "java", "ref",
                           "nr_cpus", "mem"]

        self.output_keys = ["gvcf", "gvcf_idx"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("BQSR_report",        is_required=True)
        self.add_argument("gatk",               is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=8)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 6")
        self.add_argument("location")
        self.add_argument("excluded_location")

    def define_output(self, platform, split_name=None):
        # Declare GVCF output filename
        gvcf = self.generate_unique_file_name(split_name=split_name, extension=".g.vcf")
        self.add_output(platform, "gvcf", gvcf)
        # Declare GVCF index output filename
        gvcf_idx = self.generate_unique_file_name(split_name=split_name, extension=".g.vcf.idx")
        self.add_output(platform, "gvcf_idx", gvcf_idx)

    def define_command(self, platform):
        # Get input arguments
        bam     = self.get_arguments("bam").get_value()
        BQSR    = self.get_arguments("BQSR_report").get_value()
        gatk    = self.get_arguments("gatk").get_value()
        java    = self.get_arguments("java").get_value()
        ref     = self.get_arguments("ref").get_value()
        L       = self.get_arguments("location").get_value()
        XL      = self.get_arguments("excluded_location").get_value()
        nr_cpus = self.get_arguments("nr_cpus").get_value()
        mem     = self.get_arguments("mem").get_value()

        # Get output file
        gvcf    = self.get_output("gvcf")

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 9 / 10, platform.get_workspace_dir("tmp"))

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("-o %s" % gvcf)
        opts.append("-nct %d" % nr_cpus)
        opts.append("-R %s" % ref)
        opts.append("-ERC GVCF")
        if BQSR is not None:
            opts.append("-BQSR %s" % BQSR)

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

        # Generating command for base recalibration
        cmd = "%s %s -jar %s -T HaplotypeCaller %s !LOG3!" % (java,
                                                              jvm_options,
                                                              gatk,
                                                              " ".join(opts))
        return cmd
