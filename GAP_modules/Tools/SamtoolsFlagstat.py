import logging

from GAP_interfaces import Tool

__main_class__ = "SamtoolsFlagstat"

class SamtoolsFlagstat(Tool):

    def __init__(self, config, sample_data):
        super(SamtoolsFlagstat, self).__init__()

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
            logging.error("BAM flagstat could not be obtained as no bam was obtained.")
            return None
        else:
            self.bam                = kwargs.get("bam",              self.sample_data["bam"])
        self.nr_cpus                = kwargs.get("nr_cpus",          self.nr_cpus)
        self.mem                    = kwargs.get("mem",              self.mem)

        bam_prefix = self.bam.split(".")[0]

        # Generating indexing command
        flagstat_cmd = "%s flagstat %s > %s_flagstat.txt" % (self.samtools, self.bam, bam_prefix)

        # Generating the output
        self.final_output = "%s_flagstat.txt" % bam_prefix

        return flagstat_cmd
