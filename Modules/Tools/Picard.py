from Modules import Module

class MarkDuplicates(Module):

    def __init__(self, module_id, is_docker = False):
        super(MarkDuplicates, self).__init__(module_id, is_docker)
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


class CollectInsertSizeMetrics(Module):

    def __init__(self, module_id, is_docker = False):
        super(CollectInsertSizeMetrics, self).__init__(module_id, is_docker)
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


class SamToFastq(Module):
    def __init__(self, module_id, is_docker=False):
        super(SamToFastq, self).__init__(module_id, is_docker)
        self.output_keys = ["R1", "R2"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("picard",     is_required=True, is_resource=True)
        self.add_argument("java",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=6)
        self.add_argument("mem",        is_required=True, default_value=10)
        # Require java if not being run on docker
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Generate R1 reads
        R1 = self.generate_unique_file_name(extension=".R1.fastq.gz")
        self.add_output("R1", R1)

        # Generate R2 reads
        R2 = self.generate_unique_file_name(extension=".R2.fastq.gz")
        self.add_output("R2", R2)

    def define_command(self):
        # Obtain the arguments data
        bam     = self.get_argument("bam")
        picard  = self.get_argument("picard")
        mem     = self.get_argument("mem")
        R1      = self.get_output("R1")
        R2      = self.get_output("R2")

        # Generate base cmd for running locally
        if not self.is_docker:
            java = self.get_argument("java")
            # Generate JVM arguments
            jvm_options = "-Xmx{0}G -Djava.io.tmp={1}".format(mem * 4 / 5, "/tmp/")
            basecmd = "{0} {1} -jar {2}".format(java, jvm_options, picard)

        # Generate base cmd for running on docker
        else:
            basecmd = str(picard)

        # Convert to FASTQ the final values
        return "%s SamToFastq I=%s F=%s F2=%s VALIDATION_STRINGENCY=LENIENT !LOG3!" % (basecmd, bam, R1, R2)


class SortGVCF(Module):

    def __init__(self, module_id, is_docker=False):
        super(SortGVCF, self).__init__(module_id, is_docker)
        self.output_keys = ["gvcf", "gvcf_idx"]

    def define_input(self):
        self.add_argument("gvcf",               is_required=True)
        self.add_argument("gvcf_idx",           is_required=True)
        self.add_argument("picard",             is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=4)
        self.add_argument("mem",                is_required=True, default_value=25)

        # Require java if not being run on docker
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):

        # Generate a path for the GVCF file
        gvcf_out = self.generate_unique_file_name(extension=".sorted.g.vcf")
        self.add_output("gvcf", gvcf_out)

        # Generate a path for the GVCF index file
        gvcf_idx = gvcf_out + ".idx"
        self.add_output("gvcf_idx", gvcf_idx)

    def define_command(self):
        # Get input arguments
        gvcf    = self.get_argument("gvcf")
        picard  = self.get_argument("picard")
        ref     = self.get_argument("ref")

        # Get output file
        gvcf_out    = self.get_output("gvcf")

        # Set JVM options
        if not self.is_docker:
            java    = self.get_argument("java")
            mem     = self.get_argument("mem")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=/tmp/" % (mem * 9 / 10)
            picard_cmd = "{0} {1} -jar {2}".format(java, jvm_options, picard)
        else:
            picard_cmd = str(picard)

        # Generating the options
        opts = list()
        opts.append("I=%s" % gvcf)
        opts.append("O=%s" % gvcf_out)
        opts.append("SD=%s" % ref.replace(".fasta", ".dict").replace(".fa", ".dict"))

        # Generating command for base recalibration
        return "{0} SortVcf {1} !LOG3!".format(picard_cmd, " ".join(opts))
