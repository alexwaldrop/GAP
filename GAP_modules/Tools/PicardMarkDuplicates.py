from GAP_interfaces import Tool

__main_class__ = "PicardMarkDuplicates"

class PicardMarkDuplicates(Tool):

    def __init__(self, platform, tool_id):
        super(PicardMarkDuplicates, self).__init__(platform, tool_id)

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
            return None

        # Generating variables
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem*4/5, self.tmp_dir)

        # Generating the marking duplicates options
        mark_dup_opts = list()
        mark_dup_opts.append("INPUT=%s" % bam)
        mark_dup_opts.append("OUTPUT=%s" % self.output["bam"])
        mark_dup_opts.append("METRICS_FILE=%s" % self.output["MD_report"])
        mark_dup_opts.append("ASSUME_SORTED=TRUE")
        mark_dup_opts.append("REMOVE_DUPLICATES=FALSE")
        mark_dup_opts.append("VALIDATION_STRINGENCY=LENIENT")

        # Generating command for marking duplicates
        mark_dup_cmd = "%s %s -jar %s MarkDuplicates %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["picard"], " ".join(mark_dup_opts))

        return mark_dup_cmd

    def init_output_file_paths(self, **kwargs):
        bam         = kwargs.get("bam",         None)
        split_id    = kwargs.get("split_id",    None)
        is_aligned  = kwargs.get("is_aligned",  True)

        if is_aligned:
            self.generate_output_file_path("bam", "mrkdup.bam", split_id=split_id)
            self.generate_output_file_path("MD_report", "mrkdup_metrics.txt", split_id=split_id)
        else:
            self.declare_output_file_path("bam", bam)
            self.declare_output_file_path("MD_report", "")
