from Modules import Module

class PicardInsertSizeMetrics(Module):

    def __init__(self, module_id, is_docker = False):
        super(PicardInsertSizeMetrics, self).__init__(module_id, is_docker)
        self.output_keys    = ["insert_size_report", "insert_size_histogram"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("picard",             is_required=True, is_resource=True)
        self.add_argument("num_reads",          is_required=True, default_value=1000000)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=5)

        # Require java if not being run on docker
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare insert size report filename
        report_file = self.generate_unique_file_name(extension="insertsize.out")
        self.add_output("insert_size_report", report_file)

        # Declare insert size report filename
        hist_file = self.generate_unique_file_name(extension="insertsize.hist.pdf")
        self.add_output("insert_size_histogram", hist_file)

    def define_command(self):

        # Obtaining the arguments
        bam         = self.get_argument("bam")
        picard      = self.get_argument("picard")
        num_reads   = self.get_argument("num_reads")
        mem         = self.get_argument("mem")

        # Output file
        report_out  = self.get_output("insert_size_report")
        hist_out    = self.get_output("insert_size_histogram")

        # Generate base cmd for running locally
        if not self.is_docker:
            java = self.get_argument("java")
            # Generate JVM arguments
            jvm_options = "-Xmx{0}G -Djava.io.tmp={1}".format(mem * 4 / 5, "/tmp/")
            basecmd = "{0} {1} -jar {2}".format(java, jvm_options, picard)

        # Generate base cmd for running on docker
        else:
            basecmd = str(picard)

        # Generate cmd to run picard insert size metrics
        cmd = "{0} CollectInsertSizeMetrics HISTOGRAM_FILE={1} INPUT={2} OUTPUT={3} STOP_AFTER={4} !LOG2!".format\
                (basecmd, hist_out, bam, report_out, num_reads)

        return cmd
