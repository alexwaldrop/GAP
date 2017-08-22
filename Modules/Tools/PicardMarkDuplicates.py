from Modules import Module

class PicardMarkDuplicates(Module):

    def __init__(self, module_id):
        super(PicardMarkDuplicates, self).__init__(module_id)

        self.input_keys             = ["bam", "bam_idx", "picard", "java", "nr_cpus", "mem", "is_aligned"]
        self.output_keys            = ["bam", "MD_report", "bam_sorted"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("is_aligned", is_required=True, default_value=True)
        self.add_argument("picard",     is_required=True, is_resource=True)
        self.add_argument("java",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=2)
        self.add_argument("mem",        is_required=True, default_value=10)

    def define_output(self, platform, split_name=None):

        if self.get_arguments("is_aligned").get_value():
            # Declare bam output filename
            bam = self.generate_unique_file_name(split_name=split_name, extension=".mrkdup.bam")
            self.add_output(platform, "bam", bam)

            # Declare mark duplicate report filename
            md_report = self.generate_unique_file_name(split_name=split_name, extension=".mrkdup.metrics.txt")
            self.add_output(platform, "MD_report", md_report)
        else:
            # If not 'is_aligned' return dummy files
            self.add_output(platform, "bam", self.get_arguments("bam").get_value())
            self.add_output(platform, "MD_report", None, is_path=False)

        # Specify that bam output is sorted
        self.add_output(platform, "bam_sorted", True, is_path=False)

    def define_command(self, platform):
        # Get input arguments
        bam         = self.get_arguments("bam").get_value()
        is_aligned  = self.get_arguments("is_aligned").get_value()
        mem         = self.get_arguments("mem").get_value()
        picard      = self.get_arguments("picard").get_value()
        java        = self.get_arguments("java").get_value()

        # Get output filenames
        output_bam  = self.get_output("bam")
        md_report   = self.get_output("MD_report")

        # If the obtained bam contains unaligned reads, skip the process
        if not is_aligned:
            return None

        # Generate JVM arguments
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem*4/5, platform.get_workspace_dir("tmp"))

        # Generate MarkDuplicates options
        mark_dup_opts = list()
        mark_dup_opts.append("INPUT=%s" % bam)
        mark_dup_opts.append("OUTPUT=%s" % output_bam)
        mark_dup_opts.append("METRICS_FILE=%s" % md_report)
        mark_dup_opts.append("ASSUME_SORTED=TRUE")
        mark_dup_opts.append("REMOVE_DUPLICATES=FALSE")
        mark_dup_opts.append("VALIDATION_STRINGENCY=LENIENT")

        # Generating command for marking duplicates
        cmd = "%s %s -jar %s MarkDuplicates %s !LOG3!" % (java,
                                                          jvm_options,
                                                          picard,
                                                          " ".join(mark_dup_opts))
        return cmd
