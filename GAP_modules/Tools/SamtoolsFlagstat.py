import logging

from GAP_interfaces import Tool

__main_class__ = "SamtoolsFlagstat"

class SamtoolsFlagstat(Tool):

    def __init__(self, config, sample_data):
        super(SamtoolsFlagstat, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.samtools       = self.config["paths"]["tools"]["samtools"]

        self.temp_dir       = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = False

        self.nr_cpus        = self.config["platform"]["MS_nr_cpus"]
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["bam"]
        self.output_keys    = ["flagstat"]

        self.bam            = None

    def get_command(self, **kwargs):

        self.bam                    = kwargs.get("bam",              None)
        self.nr_cpus                = kwargs.get("nr_cpus",          self.nr_cpus)
        self.mem                    = kwargs.get("mem",              self.mem)

        bam_prefix = self.bam.split(".")[0]

        # Generating indexing command
        flagstat_cmd = "%s flagstat %s > %s_flagstat.txt" % (self.samtools, self.bam, bam_prefix)

        # Generating the output
        self.output = dict()
        self.output["flagstat"] = "%s_flagstat.txt" % bam_prefix

        return flagstat_cmd
