import logging
import hashlib
import time

from GAP_interfaces import Merger

__main_class__ = "GATKCatVariants"

class GATKCatVariants(Merger):

    def __init__(self, config, sample_data):
        super(GATKCatVariants, self).__init__(config, sample_data)

        self.temp_dir = self.config["paths"]["instance_tmp_dir"]

        self.nr_cpus      = self.config["platform"]["MS_nr_cpus"]
        self.mem          = self.config["platform"]["MS_mem"]

        self.input_keys   = ["gvcf"]
        self.output_keys  = ["gvcf", "gvcf_idx"]

        self.req_tools      = ["gatk", "java"]
        self.req_resources  = ["ref"]

        self.gvcf_list    = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.gvcf_list      = kwargs.get("gvcf",            None)
        self.nr_cpus        = kwargs.get("nr_cpus",         self.nr_cpus)
        self.mem            = kwargs.get("mem",             self.mem)

        if self.gvcf_list is None:
            logging.error("Cannot merge as no inputs were received. Check if the previous module does return the gvcf paths to merge.")
            return None

        # Generating variables
        gvcf = "%s/%s_%s.g.vcf" % (self.temp_dir, self.sample_data["sample_name"], hashlib.md5(str(time.time())).hexdigest()[:5])
        gvcf_idx = "%s.idx" % gvcf
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (self.mem * 4 / 5, self.temp_dir)

        # Generating the combine options
        opts = list()
        opts.append("-out %s" % gvcf)
        opts.append("-R %s" % self.resources["ref"])
        for gvcf_input in self.gvcf_list:
            opts.append("-V %s" % gvcf_input)

        # Generating the combine command
        comb_cmd = "%s %s -cp %s org.broadinstitute.gatk.tools.CatVariants %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["gatk"], " ".join(opts))

        # Generating the output path
        self.output = dict()
        self.output["gvcf"]     = gvcf
        self.output["gvcf_idx"] = gvcf_idx

        return comb_cmd