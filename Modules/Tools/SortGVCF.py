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
        self.add_argument("nr_cpus",            is_required=True, default_value=32)
        self.add_argument("mem",                is_required=True, default_value=160)

    def define_output(self, platform, split_name=None):

        # Obtain list of input gvcfs
        gvcfs_in = self.get_arguments("gvcf").get_value()

        # Generate the lists of sorted gvcfs
        gvcfs_out = []
        gvcfs_out_idx = []
        for gvcf_in in gvcfs_in:
            gvcf_out = gvcf_in.replace(".g.vcf", ".sorted.g.vcf")
            gvcf_out_idx = gvcf_out + ".idx"

            gvcfs_out.append(gvcf_out)
            gvcfs_out_idx.append(gvcf_out_idx)

        # Define the output of the tool
        self.add_output(platform, "gvcf", gvcfs_out, is_path=False)

        self.add_output(platform, "gvcf_idx", gvcfs_out_idx, is_path=False)

    def define_command(self, platform):
        # Get input arguments
        gvcf    = self.get_arguments("gvcf").get_value()
        picard  = self.get_arguments("picard").get_value()
        java    = self.get_arguments("java").get_value()
        nr_cpus = self.get_arguments("nr_cpus").get_value()
        ref     = self.get_arguments("ref").get_value()

        # Get output file
        gvcfs_out    = self.get_output("gvcf")

        # Set JVM options
        jvm_options = "-Djava.io.tmpdir=%s" % (platform.get_workspace_dir("tmp"))

        final_cmd = ""

        for count, (gvcf_in, gvcf_out) in enumerate(zip(gvcf, gvcfs_out)):
            # Generating the options
            opts = list()
            opts.append("I=%s" % gvcf_in)
            opts.append("O=%s" % gvcf_out)
            opts.append("SD=%s" % ref.replace(".fasta", ".dict").replace(".fa", ".dict"))

            # Generating command for base recalibration
            cmd = "%s %s -jar %s SortVcf %s !LOG3!" % (java, jvm_options, picard, " ".join(opts))

            # Wait for a batch or nr_cpus to finish
            final_cmd += cmd
            final_cmd += " & "
            if count and count % (nr_cpus-1) == 0:
                final_cmd += " wait; "

        # Wait for all of them
        final_cmd += " wait;"

        return final_cmd
