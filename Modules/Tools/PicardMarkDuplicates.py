from Modules import Module

class PicardMarkDuplicates(Module):

    def __init__(self, module_id, is_docker = False):
        super(PicardMarkDuplicates, self).__init__(module_id, is_docker)
        self.output_keys            = ["bam", "MD_report", "bam_sorted"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("is_aligned", is_required=True, default_value=True)
        self.add_argument("picard",     is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=2)
        self.add_argument("mem",        is_required=True, default_value=10)

        # Require java if not being run on docker
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):

        if self.get_argument("is_aligned"):
            # Declare bam output filename
            bam = self.generate_unique_file_name(extension=".mrkdup.bam")
            self.add_output("bam", bam)

            # Declare mark duplicate report filename
            md_report = self.generate_unique_file_name(extension=".mrkdup.metrics.txt")
            self.add_output("MD_report", md_report)
        else:
            # If not 'is_aligned' return dummy files
            self.add_output("bam", self.get_argument("bam"))
            self.add_output("MD_report", None, is_path=False)

        # Specify that bam output is sorted
        self.add_output("bam_sorted", True, is_path=False)

    def define_command(self):
        # Get input arguments
        bam         = self.get_argument("bam")
        is_aligned  = self.get_argument("is_aligned")
        mem         = self.get_argument("mem")
        picard      = self.get_argument("picard")

        # Get output filenames
        output_bam  = self.get_output("bam")
        md_report   = self.get_output("MD_report")

        # If the obtained bam contains unaligned reads, skip the process
        if not is_aligned:
            return None

        # Generate base cmd for running locally
        if not self.is_docker:
            java = self.get_argument("java")
            # Generate JVM arguments
            jvm_options = "-Xmx{0:d}G -Djava.io.tmp={1}".format(mem * 4 / 5, "/tmp/")
            basecmd = "{0} {1} -jar {2}".format(java, jvm_options, picard)

        # Generate base cmd for running on docker
        else:
            basecmd = str(picard)

        # Generate MarkDuplicates options
        mark_dup_opts = list()
        mark_dup_opts.append("INPUT={0}".format(bam))
        mark_dup_opts.append("OUTPUT={0}".format(output_bam))
        mark_dup_opts.append("METRICS_FILE={0}".format(md_report))
        mark_dup_opts.append("ASSUME_SORTED=TRUE")
        mark_dup_opts.append("REMOVE_DUPLICATES=FALSE")
        mark_dup_opts.append("VALIDATION_STRINGENCY=LENIENT")

        # Generating command for marking duplicates
        cmd = "{0} MarkDuplicates {1} !LOG3!".format(basecmd, " ".join(mark_dup_opts))

        return cmd
