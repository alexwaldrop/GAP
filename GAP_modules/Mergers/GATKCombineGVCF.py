import logging
import hashlib
import time

from GAP_interfaces import Merger

__main_class__ = "GATKCombineGVCF"

class GATKCombineGVCF(Merger):

    def __init__(self, config, sample_data):
        super(GATKCombineGVCF, self).__init__(config, sample_data)

        self.nr_cpus      = self.main_server_nr_cpus
        self.mem          = self.main_server_mem

        self.input_keys   = ["gvcf"]
        self.output_keys  = ["gvcf", "gvcf_idx"]

        self.req_tools      = ["gatk", "java"]
        self.req_resources  = ["ref"]

    def get_command(self, **kwargs):

        # Obtaining the arguments
        gvcf_list      = kwargs.get("gvcf",            None)
        mem            = kwargs.get("mem",             self.mem)

        if gvcf_list is None:
            logging.error("Cannot merge as no inputs were received. Check if the previous module does return the gvcf paths to merge.")
            return None

        # Generating variables
        gvcf = "%s/%s_%s.g.vcf" % (self.tmp_dir, self.sample_data["sample_name"], hashlib.md5(str(time.time())).hexdigest()[:5])
        gvcf_idx = "%s.idx" % gvcf
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, self.tmp_dir)

        # Generating the combine options
        opts = list()
        opts.append("-o %s" % gvcf)
        opts.append("-R %s" % self.resources["ref"])
        for gvcf_input in gvcf_list:
            opts.append("-V %s" % gvcf_input)

        # Generating the combine command
        comb_cmd = "%s %s -jar %s -T CombineGVCFs %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["gatk"], " ".join(opts))

        # Generating the output path
        self.output = dict()
        self.output["gvcf"]     = gvcf
        self.output["gvcf_idx"] = gvcf_idx

        return comb_cmd