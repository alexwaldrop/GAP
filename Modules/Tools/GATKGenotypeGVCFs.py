from Modules import Module

class GATKGenotypeGVCFs(Module):

    def __init__(self, module_id):
        super(GATKGenotypeGVCFs, self).__init__(module_id)

        self.input_keys = ["gvcf", "gvcf_idx", "gatk", "java", "ref",
                           "nr_cpus", "mem", "location", "excluded_location"]

        self.output_keys = ["vcf", "vcf_idx"]

    def define_input(self):
        self.add_argument("gvcf",                is_required=True)
        self.add_argument("gvcf_idx",            is_required=True)
        self.add_argument("gatk",               is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=6)
        self.add_argument("mem",                is_required=True, default_value=35)
        self.add_argument("location")
        self.add_argument("excluded_location")

    def define_output(self, platform, split_name=None):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(split_name=split_name, extension=".vcf")
        self.add_output(platform, "vcf", vcf)
        # Declare VCF index output filename
        vcf_idx = self.generate_unique_file_name(split_name=split_name, extension=".vcf.idx")
        self.add_output(platform, "vcf_idx", vcf_idx)

    def define_command(self, platform):
        # Get input arguments
        gvcf_in = self.get_arguments("gvcf").get_value()
        gatk    = self.get_arguments("gatk").get_value()
        java    = self.get_arguments("java").get_value()
        ref     = self.get_arguments("ref").get_value()
        L       = self.get_arguments("location").get_value()
        XL      = self.get_arguments("excluded_location").get_value()
        mem     = self.get_arguments("mem").get_value()

        # Get output file
        vcf    = self.get_output("vcf")

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 9 / 10, platform.get_workspace_dir("tmp"))

        # Generating the haplotype caller options
        opts = list()

        if isinstance(gvcf_in, list):
            for gvcf in gvcf_in:
                opts.append("--variant %s" % gvcf)
        else:
            opts.append("--variant %s" % gvcf_in)
        opts.append("-o %s" % vcf)
        opts.append("-R %s" % ref)

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
        cmd = "touch *.idx; %s %s -jar %s -T GenotypeGVCFs %s !LOG3!" % (java,
                                                            jvm_options,
                                                            gatk,
                                                            " ".join(opts))
        return cmd