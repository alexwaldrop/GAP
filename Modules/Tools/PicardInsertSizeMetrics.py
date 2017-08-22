from Modules import Module

class PicardInsertSizeMetrics(Module):

    def __init__(self, module_id):
        super(PicardInsertSizeMetrics, self).__init__(module_id)

        self.input_keys     = ["bam", "bam_idx", "picard", "java", "num_reads", "nr_cpus", "mem"]
        self.output_keys    = ["insert_size_report", "insert_size_histogram"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("picard",             is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("num_reads",          is_required=True, default_value=1000000)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=5)

    def define_output(self, platform, split_name=None):
        # Declare insert size report filename
        report_file = self.generate_unique_file_name(split_name=split_name, extension="insertsize.out")
        self.add_output(platform, "insert_size_report", report_file)

        # Declare insert size report filename
        hist_file = self.generate_unique_file_name(split_name=split_name, extension="insertsize.hist.pdf")
        self.add_output(platform, "insert_size_histogram", hist_file)

    def define_command(self, platform):

        # Obtaining the arguments
        bam         = self.get_arguments("bam").get_value()
        picard      = self.get_arguments("picard").get_value()
        java        = self.get_arguments("java").get_value()
        num_reads   = self.get_arguments("num_reads").get_value()
        mem         = self.get_arguments("mem").get_value()

        # Output file
        report_out  = self.get_output("insert_size_report")
        hist_out    = self.get_output("insert_size_histogram")

        # Generate JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, platform.get_workspace_dir("tmp"))

        # Generate cmd to run picard insert size metrics
        cmd = "%s %s -jar %s CollectInsertSizeMetrics HISTOGRAM_FILE=%s INPUT=%s OUTPUT=%s STOP_AFTER=%d !LOG2!" \
                     % (java, jvm_options, picard,
                        hist_out, bam,
                        report_out,
                        num_reads)
        return cmd
