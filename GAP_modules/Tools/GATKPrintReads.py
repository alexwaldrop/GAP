from GAP_interfaces import Tool

__main_class__ = "GATKPrintReads"

class GATKPrintReads(Tool):

    def __init__(self, config, sample_data):
        super(GATKPrintReads, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.java = self.config["paths"]["java"]
        self.GATK = self.config["paths"]["gatk"]

        self.ref = self.config["paths"]["ref"]

        self.temp_dir = self.config["general"]["temp_dir"]

        self.can_split      = True
        self.splitter       = "GATKReferenceSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.nr_cpus    = 2
        self.mem        = 5


    def get_command(self, **kwargs):
        # Obtaining the arguments
        self.bam            = kwargs.get("bam",               self.sample_data["bam"])
        self.L              = kwargs.get("location",          None)
        self.XL             = kwargs.get("excluded_location", None)
        self.BQSR           = kwargs.get("BQSR_report",       self.sample_data["BQSR_report"])
        self.nr_cpus        = kwargs.get("nr_cpus",           self.nr_cpus)
        self.mem            = kwargs.get("mem",               self.mem)
        self.split_id       = kwargs.get("split_id",          None)

        # Generating variables
        bam_prefix = self.bam.split(".")[0]
        if self.split_id is None:
            recalib_bam = "%s_recalib.bam" % bam_prefix
            recalib_bam_idx = "%s_recalib.bai" % bam_prefix
        else:
            recalib_bam = "%s_recalib_%d.bam" % (bam_prefix, self.split_id)
            recalib_bam_idx = "%s_recalib_%d.bai" % (bam_prefix, self.split_id)
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (self.mem * 4 / 5, self.temp_dir)

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I %s" % self.bam)
        opts.append("-o %s" % recalib_bam)
        opts.append("-nct %d" % self.nr_cpus)
        opts.append("-R %s" % self.ref)
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
        pr_cmd = "%s %s -jar %s -T PrintReads %s !LOG3!" % (self.java, jvm_options, self.GATK, " ".join(opts))

        # Create output path
        if self.split_id is None:
            self.sample_data["bam"] = recalib_bam
            self.sample_data["bam_index"] = recalib_bam_idx
        self.output = recalib_bam

        return pr_cmd