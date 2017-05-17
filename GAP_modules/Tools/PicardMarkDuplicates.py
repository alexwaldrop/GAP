from GAP_interfaces import Tool

__main_class__ = "PicardMarkDuplicates"

class PicardMarkDuplicates(Tool):

    def __init__(self, config, sample_data):
        super(PicardMarkDuplicates, self).__init__(config, sample_data)

        self.can_split      = True
        self.splitter       = "BAMChromosomeSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.nr_cpus        = 2
        self.mem            = 10

        self.input_keys             = ["bam"]
        self.splitted_input_keys    = ["bam", "is_aligned"]
        self.output_keys            = ["bam", "MD_report"]
        self.splitted_output_keys   = ["bam", "MD_report"]

        self.req_tools      = ["picard", "java"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Obtaining the arguments
        bam        = kwargs.get("bam",         None)
        is_aligned = kwargs.get("is_aligned",  True)
        mem        = kwargs.get("mem",         self.mem)

        # If the obtained bam contains unaligned reads, skip the process
        if not is_aligned:
            self.output = dict()
            self.output["bam"] = bam
            self.output["MD_report"] = ""
            return None

        # Generating variables
        bam_prefix = bam.split(".")[0]
        bam_marked = "%s_marked.bam" % bam_prefix
        metrics    = "%s_metrics.txt" % bam_prefix
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem*4/5, self.tmp_dir)

        # Generating the marking duplicates options
        mark_dup_opts = list()
        mark_dup_opts.append("INPUT=%s" % bam)
        mark_dup_opts.append("OUTPUT=%s" % bam_marked)
        mark_dup_opts.append("METRICS_FILE=%s" % metrics)
        mark_dup_opts.append("ASSUME_SORTED=TRUE")
        mark_dup_opts.append("REMOVE_DUPLICATES=FALSE")
        mark_dup_opts.append("VALIDATION_STRINGENCY=LENIENT")

        # Generating command for marking duplicates
        mark_dup_cmd = "%s %s -jar %s MarkDuplicates %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["picard"], " ".join(mark_dup_opts))

        # Generating the output path
        self.output = dict()
        self.output["bam"] = bam_marked
        self.output["MD_report"] = metrics

        return mark_dup_cmd
