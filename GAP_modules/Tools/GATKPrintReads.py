from GAP_interfaces import Tool

__main_class__ = "GATKPrintReads"

class GATKPrintReads(Tool):

    def __init__(self, config, sample_data):
        super(GATKPrintReads, self).__init__(config, sample_data)

        self.temp_dir = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = True
        self.splitter       = "GATKReferenceSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.nr_cpus    = 2
        self.mem        = 5

        self.input_keys             = ["bam", "BQSR_report"]
        self.splitted_input_keys    = ["bam", "BQSR_report", "location", "excluded_location"]
        self.output_keys            = ["bam"]
        self.splitted_output_keys   = ["bam"]

        self.req_tools      = ["gatk", "java"]
        self.req_resources  = ["ref"]

        self.bam        = None
        self.L          = None
        self.XL         = None
        self.BQSR       = None
        self.split_id   = None

    def get_command(self, **kwargs):
        # Obtaining the arguments
        self.bam            = kwargs.get("bam",               None)
        self.L              = kwargs.get("location",          None)
        self.XL             = kwargs.get("excluded_location", None)
        self.BQSR           = kwargs.get("BQSR_report",       None)
        self.nr_cpus        = kwargs.get("nr_cpus",           self.nr_cpus)
        self.mem            = kwargs.get("mem",               self.mem)
        self.split_id       = kwargs.get("split_id",          None)

        # Generating variables
        bam_prefix = self.bam.split(".")[0]
        if self.split_id is None:
            recalib_bam = "%s_recalib.bam" % bam_prefix
        else:
            recalib_bam = "%s_recalib_%d.bam" % (bam_prefix, self.split_id)
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (self.mem * 4 / 5, self.temp_dir)

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I %s" % self.bam)
        opts.append("-o %s" % recalib_bam)
        opts.append("-nct %d" % self.nr_cpus)
        opts.append("-R %s" % self.resources["ref"])
        opts.append("-BQSR %s" % self.BQSR)

        # Limit the locations to be processes
        if self.L is not None:
            if isinstance(self.L, list):
                for included in self.L:
                    opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % self.L)
        if self.XL is not None:
            if isinstance(self.XL, list):
                for excluded in self.XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % self.XL)

        # Generating command for recalibrating the BAM file
        pr_cmd = "%s %s -jar %s -T PrintReads %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["gatk"], " ".join(opts))

        # Create output path
        self.output = dict()
        self.output["bam"] = recalib_bam

        return pr_cmd