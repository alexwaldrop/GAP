from GAP_interfaces import Tool

__main_class__ = "PicardMarkDuplicates"

class PicardMarkDuplicates(Tool):

    def __init__(self, config, sample_data):
        super(PicardMarkDuplicates, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.picard         = self.config["paths"]["tools"]["picard"]
        self.java           = self.config["paths"]["tools"]["java"]

        self.temp_dir       = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = True
        self.splitter       = "BAMChromosomeSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.nr_cpus        = 2
        self.mem            = 10

        self.bam            = None
        self.is_aligned     = None
        self.split_id       = None

        self.input_keys             = ["bam"]
        self.splitted_input_keys    = ["bam", "is_aligned"]
        self.output_keys            = ["bam", "MD_report"]
        self.splitted_output_keys   = ["bam", "MD_report"]

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.bam        = kwargs.get("bam",         None)
        self.is_aligned = kwargs.get("is_aligned",  True)
        self.nr_cpus    = kwargs.get("nr_cpus",     self.nr_cpus)
        self.mem        = kwargs.get("mem",         self.mem)
        self.split_id   = kwargs.get("split_id",    None)

        # If the obtained bam contains unaligned reads, skip the process
        if not self.is_aligned:
            self.output = dict()
            self.output["bam"] = self.bam
            self.output["MD_report"] = ""
            return None

        # Generating variables
        bam_prefix = self.bam.split(".")[0]
        bam_marked = "%s_marked.bam" % bam_prefix
        metrics    = "%s_metrics.txt" % bam_prefix
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (self.mem*4/5, self.temp_dir)

        # Generating the marking duplicates options
        mark_dup_opts = list()
        mark_dup_opts.append("INPUT=%s" % self.bam)
        mark_dup_opts.append("OUTPUT=%s" % bam_marked)
        mark_dup_opts.append("METRICS_FILE=%s" % metrics)
        mark_dup_opts.append("ASSUME_SORTED=TRUE")
        mark_dup_opts.append("REMOVE_DUPLICATES=FALSE")
        mark_dup_opts.append("VALIDATION_STRINGENCY=LENIENT")

        # Generating command for marking duplicates
        mark_dup_cmd = "%s %s -jar %s MarkDuplicates %s !LOG3!" % (self.java, jvm_options, self.picard, " ".join(mark_dup_opts))

        # Generating the output path
        self.output = dict()
        self.output["bam"] = bam_marked
        self.output["MD_report"] = metrics

        return mark_dup_cmd
