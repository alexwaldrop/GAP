from Modules import Module

class SortGVCF(Module):

    def __init__(self, module_id):
        super(SortGVCF, self).__init__(module_id)

        self.input_keys = ["gvcf", "gvcf_idx", "picard", "java", "ref",
                           "nr_cpus", "mem"]

        self.output_keys = ["gvcf", "gvcf_idx"]

    def define_input(self):
        self.add_argument("gvcf",               is_required=True)
        self.add_argument("gvcf_idx",           is_required=True)
        self.add_argument("picard",             is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=4)
        self.add_argument("mem",                is_required=True, default_value=25)

    def define_output(self, platform, split_name=None):

        # Generate a path for the GVCF file
        gvcf_out = self.generate_unique_file_name(split_name=split_name, extension=".sorted.g.vcf")
        self.add_output(platform, "gvcf", gvcf_out)

        # Generate a path for the GVCF index file
        gvcf_idx = gvcf_out + ".idx"
        self.add_output(platform, "gvcf_idx", gvcf_idx)

    def define_command(self, platform):
        # Get input arguments
        gvcf    = self.get_arguments("gvcf").get_value()
        picard  = self.get_arguments("picard").get_value()
        java    = self.get_arguments("java").get_value()
        mem     = self.get_arguments("mem").get_value()
        ref     = self.get_arguments("ref").get_value()

        # Get output file
        gvcf_out    = self.get_output("gvcf")

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 9 / 10, platform.get_workspace_dir("tmp"))

        # Generating the options
        opts = list()
        opts.append("I=%s" % gvcf)
        opts.append("O=%s" % gvcf_out)
        opts.append("SD=%s" % ref.replace(".fasta", ".dict").replace(".fa", ".dict"))

        # Generating command for base recalibration
        cmd = "%s %s -jar %s SortVcf %s !LOG3!" % (java, jvm_options, picard, " ".join(opts))

        return cmd
