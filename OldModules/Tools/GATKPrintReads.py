from GAP_interfaces import Tool

__main_class__ = "GATKPrintReads"

class GATKPrintReads(Tool):

    def __init__(self, platform, tool_id):
        super(GATKPrintReads, self).__init__(platform, tool_id)

        self.can_split      = True
        self.splitter       = "GATKReferenceSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.nr_cpus    = 2
        self.mem        = 5

        self.input_keys             = ["bam", "BQSR_report", "bam_idx"]
        self.splitted_input_keys    = ["bam", "BQSR_report", "location", "excluded_location"]
        self.output_keys            = ["bam"]
        self.splitted_output_keys   = ["bam"]

        self.req_tools      = ["gatk", "java"]
        self.req_resources  = ["ref"]

    def get_command(self, **kwargs):
        # Obtaining the arguments
        bam            = kwargs.get("bam",               None)
        L              = kwargs.get("location",          None)
        XL             = kwargs.get("excluded_location", None)
        BQSR           = kwargs.get("BQSR_report",       None)
        nr_cpus        = kwargs.get("nr_cpus",           self.nr_cpus)
        mem            = kwargs.get("mem",               self.mem)

        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, self.tmp_dir)

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("-o %s" % self.output["bam"])
        opts.append("-nct %d" % nr_cpus)
        opts.append("-R %s" % self.resources["ref"])
        opts.append("-BQSR %s" % BQSR)

        # Limit the locations to be processes
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating command for recalibrating the BAM file
        pr_cmd = "%s %s -jar %s -T PrintReads %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["gatk"], " ".join(opts))

        return pr_cmd

    def init_output_file_paths(self, **kwargs):

        split_id = kwargs.get("split_id", None)
        self.generate_output_file_path(output_key="bam",
                                       extension="recalibrated.bam",
                                       split_id=split_id)
