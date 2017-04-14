import logging

from GAP_interfaces import Tool

__main_class__ = "SamtoolsIndex"

class SamtoolsIndex(Tool):

    def __init__(self, config, sample_data):
        super(SamtoolsIndex, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.samtools       = self.config["paths"]["samtools"]

        self.temp_dir       = self.config["general"]["temp_dir"]

        self.can_split      = False

        self.nr_cpus        = self.config["platform"]["MS_nr_cpus"]
        self.mem            = self.config["platform"]["MS_mem"]

        self.bam            = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        if "bam" not in self.sample_data:
            logging.error("BAM index could not be obtained as no bam was obtained.")
            return None
        else:
            self.bam                = kwargs.get("bam",              self.sample_data["bam"])
        self.nr_cpus                = kwargs.get("nr_cpus",          self.nr_cpus)
        self.mem                    = kwargs.get("mem",              self.mem)

        # Generating indexing command
        index_cmd = "%s index %s" % (self.samtools, self.bam)

        # Generating the output paths
        self.sample_data["bam_index"] = "%s.bai" % self.bam

        return index_cmd
