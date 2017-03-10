__main_class__ = "PicardMarkDuplicates"

class PicardMarkDuplicates(object):

    def __init__(self, config, sample_data):
        self.config = config
        self.sample_data = sample_data

        self.picard         = self.config["paths"]["picard"]
        self.java           = self.config["paths"]["java"]

        self.temp_dir       = self.config["general"]["temp_dir"]

        self.sample_name    = self.sample_data["sample_name"]

        self.can_split      = True
        self.splitter       = "BAMChromosomeSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.bam            = None
        self.is_aligned     = None
        self.threads        = None
        self.mem            = None

        self.output_path    = None
        self.pipeline_output_path = None

    def get_pipeline_output(self):
        return self.pipeline_output_path

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.bam        = kwargs.get("bam",         self.sample_data["bam"])
        self.is_aligned = kwargs.get("is_aligned",  True)
        self.threads    = kwargs.get("cpus",        self.config["instance"]["nr_cpus"])
        self.mem        = kwargs.get("mem",         self.config["instance"]["mem"])

        if not self.is_aligned:
            self.output_path = self.bam
            return

        # Generating variables
        bam_prefix = self.bam.split(".")[0]
        bam_marked = "%s_marked.bam" % bam_prefix
        metrics    = "%s_metrics.txt" % bam_prefix
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=/data/tmp" % (self.mem*4/5)

        # Generating the marking duplicates options
        mark_dup_opts = list()
        mark_dup_opts.append("INPUT=%s" % self.bam)
        mark_dup_opts.append("OUTPUT=%s" % bam_marked)
        mark_dup_opts.append("METRICS_FILE=%s" % metrics)
        mark_dup_opts.append("ASSUME_SORTED=TRUE")
        mark_dup_opts.append("REMOVE_DUPLICATES=FALSE")
        mark_dup_opts.append("VALIDATION_STRINGENCY=LENIENT")
        mark_dup_opts.append("MAX_RECORDS_IN_RAM=5000000")
        mark_dup_opts.append("TMP_DIR=%s" % self.temp_dir)

        # Generating command for marking duplicates
        mark_dup_cmd = "%s %s -jar %s MarkDuplicates %s !LOG3!" % (self.java, jvm_options, self.picard, " ".join(mark_dup_opts))

        # Generating the output path
        self.output_path = bam_marked
        self.pipeline_output_path = [metrics]

        return mark_dup_cmd
