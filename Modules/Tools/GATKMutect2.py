from Modules import Module

class GATKMutect2(Module):

    def __init__(self, module_id):
        super(GATKMutect2, self).__init__(module_id)

        self.input_keys = ["bam", "bam_idx", "is_tumor",
                           "location", "excluded_location",
                           "gatk", "java", "ref", "germline_vcf",
                           "nr_cpus", "mem"]

        self.output_keys = ["vcf", "vcf_idx"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("is_tumor",           is_required=True)
        self.add_argument("gatk",               is_required=True,   is_resource=True)
        self.add_argument("java",               is_required=True,   is_resource=True)
        self.add_argument("ref",                is_required=True,   is_resource=True)
        self.add_argument("germline_vcf",       is_required=False,  is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=8)
        self.add_argument("mem",                is_required=True,   default_value=48)
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
        bams            = self.get_arguments("bam").get_value()
        sample_names    = self.get_arguments("sample_name").get_value()
        is_tumor        = self.get_arguments("is_tumor").get_value()
        gatk            = self.get_arguments("gatk").get_value()
        java            = self.get_arguments("java").get_value()
        ref             = self.get_arguments("ref").get_value()
        germline_vcf    = self.get_arguments("germline_vcf").get_value()
        L               = self.get_arguments("location").get_value()
        XL              = self.get_arguments("excluded_location").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        mem             = self.get_arguments("mem").get_value()

        # Get output file
        vcf    = self.get_output("vcf")

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, platform.get_workspace_dir("tmp"))

        # Generating the MuTect2 options
        opts = list()

        # Add Tumor/Normal sample names
        if is_tumor[0]:
            opts.append("-tumor %s" % sample_names[0])
            opts.append("-normal %s" % sample_names[1])
        else:
            opts.append("-tumor %s" % sample_names[1])
            opts.append("-normal %s" % sample_names[0])

        # Add arguments for bams
        tumor_bams = ["-I %s" % bam for bam in bams[0] ] if isinstance(bams[0], list) else ["-I %s" % bams[0]]
        normal_bams = ["-I %s" % bam for bam in bams[1] ] if isinstance(bams[1], list) else ["-I %s" % bams[1]]
        opts += tumor_bams + normal_bams

        opts.append("-O %s" % vcf)
        opts.append("-R %s" % ref)
        opts.append("--native-pair-hmm-threads %s" % nr_cpus)

        if germline_vcf is not None:
            opts.append("--germline-resource %s" % germline_vcf)

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
        cmd = "%s %s -jar %s Mutect2 %s !LOG3!" % (java, jvm_options, gatk, " ".join(opts))
        return cmd
