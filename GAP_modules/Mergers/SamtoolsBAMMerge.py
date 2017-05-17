import hashlib
import time
import logging

from GAP_interfaces import Merger

__main_class__ = "SamtoolsBAMMerge"

class SamtoolsBAMMerge(Merger):

    def __init__(self, config, sample_data):
        super(SamtoolsBAMMerge, self).__init__(config, sample_data)

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

        # Generating the output
        bam_output = "%s/%s_%s.bam" % (self.tmp_dir, self.sample_name, hashlib.md5(str(time.time())).hexdigest()[:5])
        self.output = dict()
        self.output["bam"] = bam_output

        # Generating the merging command
        if sorted_input:
            bam_merge_cmd = "%s merge -c -@%d %s %s" % (self.tools["samtools"], nr_cpus, bam_output, " ".join(bam_list))
        else:
            bam_merge_cmd = "%s cat -o %s %s" % (self.tools["samtools"], bam_output, " ".join(bam_list))

        return bam_merge_cmd
