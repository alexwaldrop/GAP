from GAP_interfaces import Tool

__main_class__ = "GATKHaplotypeCaller"

class GATKHaplotypeCaller(Tool):

    def __init__(self, config, sample_data):
        super(GATKHaplotypeCaller, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.java = self.config["paths"]["java"]
        self.GATK = self.config["paths"]["gatk"]

        self.ref = self.config["paths"]["ref"]

        self.temp_dir = self.config["general"]["temp_dir"]

        self.can_split      = True
        self.splitter       = "GATKReferenceSplitter"
        self.merger         = "GATKCatVariants"

        self.nr_cpus    = 8
        self.mem        = self.nr_cpus * 4 # 4GB/vCPU

        self.bam = None
        self.L = None
        self.XL = None
        self.BQSR = None
        self.split_id = None

    def get_command(self, **kwargs):
        # Obtaining the arguments
        self.bam            = kwargs.get("bam",               self.sample_data["bam"])
        self.L              = kwargs.get("location",          None)
        self.XL             = kwargs.get("excluded_location", None)
        self.nr_cpus        = kwargs.get("nr_cpus",           self.nr_cpus)
        self.mem            = kwargs.get("mem",               self.mem)
        self.split_id       = kwargs.get("split_id",          None)
        if "BQSR_report" in self.sample_data:
            self.BQSR = kwargs.get("BQSR_report",             self.sample_data["BQSR_report"])
        else:
            self.BQSR = kwargs.get("BQSR_report",             None)

        # Generating variables
        bam_prefix = self.bam.split(".")[0]
        if self.split_id is not None:
            gvcf = "%s_%d.g.vcf" % (bam_prefix, self.split_id)
            idx = "%s.idx" % gvcf
        else:
            gvcf = "%s.g.vcf" % bam_prefix
            idx = "%s.idx" % gvcf
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (self.mem * 4 / 5, self.temp_dir)

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I %s" % self.bam)
        opts.append("-o %s" % gvcf)
        opts.append("-nct %d" % self.nr_cpus)
        opts.append("-R %s" % self.ref)
        opts.append("-ERC GVCF")
        if self.BQSR is not None:
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

        # Generating command for base recalibration
        hc_cmd = "%s %s -jar %s -T HaplotypeCaller %s !LOG3!" % (self.java, jvm_options, self.GATK, " ".join(opts))

        # Create output path
        if self.split_id is None:
            self.final_output = [gvcf, idx]
        else:
            self.output = gvcf

        return hc_cmd