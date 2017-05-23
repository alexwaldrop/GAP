import hashlib
import time
import logging

from GAP_interfaces import Merger

__main_class__ = "SamtoolsBAMMerge"

class SamtoolsBAMMerge(Merger):

    def __init__(self, config, sample_data, tool_id, main_module_name=None):
        super(SamtoolsBAMMerge, self).__init__(config, sample_data, tool_id, main_module_name)

        self.nr_cpus      = self.main_server_nr_cpus
        self.mem          = self.main_server_mem

        self.input_keys   = ["bam"]
        self.output_keys  = ["bam"]

        self.req_tools      = ["samtools"]
        self.req_resources  = []

        self.sample_name  = self.sample_data["sample_name"]

    def get_command(self, **kwargs):

        # Obtaining the arguments
        nr_cpus        = kwargs.get("nr_cpus",         self.nr_cpus)
        bam_list       = kwargs.get("bam",             None)
        sorted_input   = kwargs.get("sorted_input",    True)

        if bam_list is None:
            logging.error("Cannot merge as no inputs were received. Check if the previous module does return the bam paths to merge.")
            return None

        # Generating the merging command
        if sorted_input:
            bam_merge_cmd = "%s merge -c -@%d %s %s" % (self.tools["samtools"], nr_cpus, self.output["bam"], " ".join(bam_list))
        else:
            bam_merge_cmd = "%s cat -o %s %s" % (self.tools["samtools"], self.output["bam"], " ".join(bam_list))

        return bam_merge_cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("bam", "bam")
