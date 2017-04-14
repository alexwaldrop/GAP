import logging
import hashlib
import time

from GAP_interfaces import Merger

__main_class__ = "GATKCatVariants"

class GATKCatVariants(Merger):

    def __init__(self, config, sample_data):
        super(GATKCatVariants, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.java = self.config["paths"]["java"]
        self.GATK = self.config["paths"]["gatk"]

        self.ref = self.config["paths"]["ref"]

        self.temp_dir = self.config["general"]["temp_dir"]

        self.nr_cpus      = self.config["platform"]["MS_nr_cpus"]
        self.mem          = self.config["platform"]["MS_mem"]

        self.inputs       = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.inputs         = kwargs.get("inputs",          None)
        self.nr_cpus        = kwargs.get("nr_cpus",         self.nr_cpus)
        self.mem            = kwargs.get("mem",             self.mem)

        if self.inputs is None:
            logging.error("Cannot merge as no inputs were received. Check if the previous module does return the bam paths to merge.")
            return None

        # Generating variables
        gvcf = "%s/%s_%s.g.vcf" % (self.temp_dir, self.sample_data["sample_name"], hashlib.md5(str(time.time())).hexdigest()[:5])
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=/data/tmp" % (self.mem * 4 / 5)

        # Generating the combine options
        opts = list()
        opts.append("-out %s" % gvcf)
        opts.append("-R %s" % self.ref)
        for gvcf_input in self.inputs:
            opts.append("-V %s" % gvcf_input)

        # Generating the combine command
        comb_cmd = "%s %s -cp %s org.broadinstitute.gatk.tools.CatVariants %s !LOG3!" % (self.java, jvm_options, self.GATK, " ".join(opts))

        # Generating the output path
        self.final_output = gvcf

        return comb_cmd