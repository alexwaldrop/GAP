import logging
import hashlib
import time

from GAP_interfaces import Merger

__main_class__ = "GATKCatVariants"

class GATKCatVariants(Merger):

    def __init__(self, platform, tool_id, main_module_name=None):
        super(GATKCatVariants, self).__init__(platform, tool_id, main_module_name)

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
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, self.tmp_dir)

        # Generating the combine options
        opts = list()
        opts.append("-out %s" % self.output["gvcf"])
        opts.append("-R %s" % self.resources["ref"])
        for gvcf_input in gvcf_list:
            opts.append("-V %s" % gvcf_input)

        # Generating the combine command
        comb_cmd = "%s %s -cp %s org.broadinstitute.gatk.tools.CatVariants %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["gatk"], " ".join(opts))

        return comb_cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("gvcf", "g.vcf")
        self.generate_output_file_path("gvcf_idx", "g.vcf.idx")