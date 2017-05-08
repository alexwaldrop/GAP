import hashlib
import time
import logging

from GAP_interfaces import Merger

__main_class__ = "SamtoolsBAMMerge"

class SamtoolsBAMMerge(Merger):

    def __init__(self, config, sample_data):
        super(SamtoolsBAMMerge, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.samtools     = self.config["paths"]["samtools"]

        self.temp_dir     = self.config["general"]["temp_dir"]

        self.sample_name  = self.sample_data["sample_name"]

        self.nr_cpus      = self.config["platform"]["MS_nr_cpus"]
        self.mem          = self.config["platform"]["MS_mem"]

        self.input_keys   = ["bam"]
        self.output_keys  = ["bam"]

        self.bam_list     = None
        self.sorted_input = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.nr_cpus        = kwargs.get("nr_cpus",         self.nr_cpus)
        self.mem            = kwargs.get("mem",             self.mem)
        self.bam_list       = kwargs.get("bam",             None)
        self.sorted_input   = kwargs.get("sorted_input",    True)

        if self.bam_list is None:
            logging.error("Cannot merge as no inputs were received. Check if the previous module does return the bam paths to merge.")
            return None

        # Generating the output
        bam_output = "%s/%s_%s.bam" % (self.temp_dir, self.sample_name, hashlib.md5(str(time.time())).hexdigest()[:5])
        self.output = dict()
        self.output["bam"] = bam_output

        # Generating the merging command
        if self.sorted_input:
            bam_merge_cmd = "%s merge -c -@%d %s %s" % (self.samtools, self.nr_cpus, bam_output, " ".join(self.bam_list))
        else:
            bam_merge_cmd = "%s cat -o %s %s" % (self.samtools, bam_output, " ".join(self.bam_list))

        return bam_merge_cmd
