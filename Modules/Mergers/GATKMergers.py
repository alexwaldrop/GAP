from Modules import Merger

class GenotypeGVCFs(Merger):

    def __init__(self, module_id, is_docker=False):
        super(GenotypeGVCFs, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf", "vcf_idx"]

    def define_input(self):
        self.add_argument("gvcf",                is_required=True)
        self.add_argument("gvcf_idx",            is_required=True)
        self.add_argument("gatk",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=6)
        self.add_argument("mem",                is_required=True, default_value=35)
        self.add_argument("location")
        self.add_argument("excluded_location")

        # Require java if not being run in docker environment
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)
        # Declare VCF index output filename
        vcf_idx = self.generate_unique_file_name(extension=".vcf.idx")
        self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Get input arguments
        gvcf_in = self.get_argument("gvcf")
        gatk    = self.get_argument("gatk")
        ref     = self.get_argument("ref")
        L       = self.get_argument("location")
        XL      = self.get_argument("excluded_location")
        vcf    = self.get_output("vcf")

        # Generate base command where executables exist outside docker
        if not self.is_docker:
            java = self.get_argument("java")
            mem = self.get_argument("mem")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, "/tmp/")
            gatk_cmd = "%s %s -jar %s -T" % (java, jvm_options, gatk)

        # Generate base command with endpoint provided by docker
        else:
            gatk_cmd = "%s" % gatk

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
        return "touch {0}/*.idx; {1} GenotypeGVCFs {2} !LOG3!".format(self.get_output_dir(), gatk_cmd, " ".join(opts))


class Mutect2(Merger):

    def __init__(self, module_id, is_docker=False):
        super(Mutect2, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf", "vcf_idx"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("is_tumor",           is_required=True)
        self.add_argument("gatk",               is_required=True,   is_resource=True)
        self.add_argument("ref",                is_required=True,   is_resource=True)
        self.add_argument("germline_vcf",       is_required=False,  is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=8)
        self.add_argument("mem",                is_required=True,   default_value=30)
        self.add_argument("location")
        self.add_argument("excluded_location")

        # Require java if not being run in docker environment
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)
        # Declare VCF index output filename
        vcf_idx = self.generate_unique_file_name(extension=".vcf.idx")
        self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Get input arguments
        bams            = self.get_argument("bam")
        sample_names    = self.get_argument("sample_name")
        is_tumor        = self.get_argument("is_tumor")
        gatk            = self.get_argument("gatk")
        ref             = self.get_argument("ref")
        germline_vcf    = self.get_argument("germline_vcf")
        L               = self.get_argument("location")
        XL              = self.get_argument("excluded_location")
        nr_cpus         = self.get_argument("nr_cpus")
        mem             = self.get_argument("mem")
        vcf             = self.get_output("vcf")

        # Generate command with java if not running on docker
        if not self.is_docker:
            java = self.get_argument("java")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, "/tmp/")
            cmd = "%s %s -jar %s Mutect2" % (java, jvm_options, gatk)

        # Generate base command with endpoint provided by docker
        else:
            cmd = "%s Mutect2" % gatk

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

        # Generating command for Mutect2
        return "%s %s !LOG3!" % (cmd, " ".join(opts))


class MergeBQSRs(Merger):

    def __init__(self, module_id, is_docker=False):
        super(MergeBQSRs, self).__init__(module_id, is_docker)
        self.output_keys  = ["BQSR_report"]

    def define_input(self):
        self.add_argument("BQSR_report",    is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")
        self.add_argument("gatk",           is_required=True, is_resource=True)

        # Require java if not being run in docker environment
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare merged bam output file
        bqsr_out = self.generate_unique_file_name(extension=".merged.grp")
        self.add_output("BQSR_report", bqsr_out)

    def define_command(self):
        # Obtaining the arguments
        bqsrs_in    = self.get_argument("BQSR_report")
        gatk        = self.get_argument("gatk")
        mem         = self.get_argument("mem")
        bqsr_out    = self.get_output("BQSR_report")

        # Generate base command where executables exist outside docker
        if not self.is_docker:
            java = self.get_argument("java")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, "/tmp/")
            gatk_cmd = "%s %s -jar %s" % (java, jvm_options, gatk)

        # Generate base command with endpoint provided by docker
        else:
            gatk_cmd = "%s" % gatk

        return "{0} GatherBQSRReports --input {1} -O {2}".format(gatk_cmd, " --input ".join(bqsrs_in), bqsr_out)


class CatVariants(Merger):
    # Merger module intended to merge gVCF files within samples (i.e. re-combine chromosomes)

    def __init__(self, module_id, is_docker=False):
        super(CatVariants, self).__init__(module_id, is_docker)
        self.output_keys  = ["gvcf", "gvcf_idx"]

    def define_input(self):
        self.add_argument("gvcf",       is_required=True)
        self.add_argument("gvcf_idx",   is_required=True)
        self.add_argument("gatk",       is_required=True, is_resource=True)
        self.add_argument("ref",        is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=2)
        self.add_argument("mem",        is_required=True, default_value=13)
        self.add_argument("java",       is_required=True, is_resource=True)

    def define_output(self):
        # Declare merged GVCF output filename
        gvcf = self.generate_unique_file_name(extension=".g.vcf")
        self.add_output("gvcf", gvcf)
        # Declare GVCF index output filename
        gvcf_idx = gvcf + ".idx"
        self.add_output("gvcf_idx", gvcf_idx)

    def define_command(self):
        # Obtaining the arguments
        gvcf_in     = self.get_argument("gvcf")
        gatk        = self.get_argument("gatk")
        java        = self.get_argument("java")
        ref         = self.get_argument("ref")
        mem         = self.get_argument("mem")
        gvcf_out    = self.get_output("gvcf")

        # Generating JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=/tmp/" % (mem * 9 / 10)

        # Generating the CatVariants options
        opts = list()
        opts.append("-out %s" % gvcf_out)
        opts.append("-R %s" % ref)
        if isinstance(gvcf_in, list):
            for gvcf_input in gvcf_in:
                opts.append("-V %s" % gvcf_input)
        else:
            opts.append("-V %s" % gvcf_in)

        # Generating the combine command
        cmd = "%s %s -cp %s org.broadinstitute.gatk.tools.CatVariants %s !LOG3!" % (java,
                                                                                    jvm_options,
                                                                                    gatk,
                                                                                    " ".join(opts))
        return cmd


class CombineGVCF(Merger):
    # Merger module intended to merge gVCF files across multiple samples
    def __init__(self, module_id):
        super(CombineGVCF, self).__init__(module_id)
        self.output_keys  = ["gvcf", "gvcf_idx"]

    def define_input(self):
        self.add_argument("gvcf",               is_required=True)
        self.add_argument("gvcf_idx",           is_required=True)
        self.add_argument("gatk",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=8)
        self.add_argument("mem",                is_required=True, default_value=16)
        self.add_argument("location")
        self.add_argument("excluded_location")

        # Require java if not being run in docker environment
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare merged GVCF output filename
        gvcf = self.generate_unique_file_name(extension=".g.vcf")
        self.add_output("gvcf", gvcf)
        # Declare GVCF index output filename
        gvcf_idx = self.generate_unique_file_name(extension=".g.vcf.idx")
        self.add_output("gvcf_idx", gvcf_idx)

    def define_command(self):

        # Obtaining the arguments
        gvcf_list   = self.get_argument("gvcf")
        gatk        = self.get_argument("gatk")
        ref         = self.get_argument("ref")
        L           = self.get_argument("location")
        XL          = self.get_argument("excluded_location")
        gvcf_out    = self.get_output("gvcf")

        # Generate base command where executables exist outside docker
        if not self.is_docker:
            java = self.get_argument("java")
            mem  = self.get_argument("mem")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, "/tmp/")
            gatk_cmd = "%s %s -jar %s -T" % (java, jvm_options, gatk)

        # Generate base command with endpoint provided by docker
        else:
            gatk_cmd = "%s" % gatk

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
        return "%s CombineGVCFs %s !LOG3!" % (gatk_cmd, " ".join(opts))
