from GAP_interfaces import Tool

__main_class__ = "GATKHaplotypeCaller"

class GATKHaplotypeCaller(Tool):

    def __init__(self, config, sample_data):
        super(GATKHaplotypeCaller, self).__init__(config, sample_data)

        self.can_split      = True
        self.splitter       = "GATKReferenceSplitter"
        self.merger         = "GATKCatVariants"

        self.nr_cpus    = 8
        self.mem        = self.nr_cpus * 4 # 4GB/vCPU

        self.input_keys             = ["bam"]
        self.splitted_input_keys    = ["bam", "BQSR_report", "location", "excluded_location"]
        self.output_keys            = ["gvcf", "gvcf_idx"]
        self.splitted_output_keys   = ["gvcf", "gvcf_idx"]

        self.req_tools      = ["gatk", "java"]
        self.req_resources  = ["ref"]

    def get_command(self, **kwargs):
        # Obtaining the arguments
        bam            = kwargs.get("bam",               None)
        BQSR           = kwargs.get("BQSR_report",       None)
        L              = kwargs.get("location",          None)
        XL             = kwargs.get("excluded_location", None)
        nr_cpus        = kwargs.get("nr_cpus",           self.nr_cpus)
        mem            = kwargs.get("mem",               self.mem)
        split_id       = kwargs.get("split_id",          None)

        # Generating variables
        bam_prefix = bam.split(".")[0]
        if split_id is not None:
            gvcf = "%s_%d.g.vcf" % (bam_prefix, split_id)
        else:
            gvcf = "%s.g.vcf" % bam_prefix
        idx = "%s.idx" % gvcf
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, self.tmp_dir)

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("-o %s" % gvcf)
        opts.append("-nct %d" % nr_cpus)
        opts.append("-R %s" % self.resources["ref"])
        opts.append("-ERC GVCF")
        if BQSR is not None:
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

        # Generating command for base recalibration
        hc_cmd = "%s %s -jar %s -T HaplotypeCaller %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["gatk"], " ".join(opts))

        # Create output path
        self.output = dict()
        self.output["gvcf"]     = gvcf
        self.output["gvcf_idx"] = idx

        return hc_cmd