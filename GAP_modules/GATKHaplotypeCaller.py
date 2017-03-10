__main_class__ = "GATKHaplotypeCaller"

class GATKHaplotypeCaller(object):

    def __init__(self, config, sample_data):
        self.config = config
        self.sample_data = sample_data

        self.java = self.config["paths"]["java"]
        self.GATK = self.config["paths"]["gatk"]

        self.ref = self.config["paths"]["ref"]

        self.can_split      = True
        self.splitter       = "GATKReferenceSplitter"
        self.merger         = "GATKCatVariants"

        self.bam = None
        self.threads = None

        self.output_path = None
        self.pipeline_output_path = None

    def get_pipeline_output(self):
        return self.pipeline_output_path

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):
        # Obtaining the arguments
        self.bam            = kwargs.get("bam",               self.sample_data["bam"])
        self.L              = kwargs.get("location",          None)
        self.XL             = kwargs.get("excluded_location", None)
        self.threads        = kwargs.get("cpus",              self.config["instance"]["nr_cpus"])
        self.mem            = kwargs.get("mem",               self.config["instance"]["mem"])
        self.split_id       = kwargs.get("split_id",          0)

        # Generating variables
        bam_prefix = self.bam.split(".")[0]
        gvcf = "%s_%d.g.vcf" % (bam_prefix, self.split_id)
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=/data/tmp" % (self.mem * 4 / 5)

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I %s" % self.bam)
        opts.append("-o %s" % gvcf)
        opts.append("-nct %d" % self.threads)
        opts.append("-R %s" % self.ref)
        opts.append("-ERC GVCF")

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
        self.output_path = gvcf

        return hc_cmd