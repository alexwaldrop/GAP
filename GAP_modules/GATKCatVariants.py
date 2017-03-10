import logging
import hashlib
import time

__main_class__ = "GATKCatVariants"

class GATKCatVariants(object):

    def __init__(self, config, sample_data):

        self.config = config
        self.sample_data = sample_data

        self.java = self.config["paths"]["java"]
        self.GATK = self.config["paths"]["gatk"]

        self.ref = self.config["paths"]["ref"]

        self.temp_dir = self.config["general"]["temp_dir"]

        self.threads      = None
        self.inputs       = None
        self.nr_splits    = None

        self.output_path  = None
        self.pipeline_output_path = None

    def get_pipeline_output(self):
        return self.pipeline_output_path

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.threads        = kwargs.get("cpus",            self.config["instance"]["nr_cpus"])
        self.inputs         = kwargs.get("inputs",          None)
        self.nr_splits      = kwargs.get("nr_splits",       2)
        self.mem            = kwargs.get("mem",             self.config["instance"]["mem"])

        if self.inputs is None:
            logging.error("Cannot merge as no inputs were received. Check if the previous module does return the bam paths to merge.")

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
        self.output_path = gvcf
        self.sample_data["gvcf"] = self.output_path
        self.pipeline_output_path = gvcf

        return comb_cmd