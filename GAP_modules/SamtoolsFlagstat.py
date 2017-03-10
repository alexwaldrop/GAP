import logging

__main_class__ = "SamtoolsFlagstat"

class SamtoolsFlagstat(object):

    def __init__(self, config, sample_data):

        self.config = config
        self.sample_data = sample_data

        self.samtools       = self.config["paths"]["samtools"]

        self.temp_dir       = self.config["general"]["temp_dir"]

        self.can_split      = False

        self.bam            = None
        self.output_path    = None
        self.pipeline_output_path = None

    def get_pipeline_output(self):
        return self.pipeline_output_path

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        if "bam" not in self.sample_data:
            logging.error("BAM flagstat could not be obtained as no bam was obtained.")
            return None
        else:
            self.bam                = kwargs.get("bam",              self.sample_data["bam"])

        bam_prefix = self.bam.split(".")[0]

        # Generating indexing command
        flagstat_cmd = "%s flagstat %s > %s_flagstat.txt" % (self.samtools, self.bam, bam_prefix)

        # Generating the output paths
        self.output_path = "%s_flagstat.txt" % bam_prefix
        self.pipeline_output_path = self.output_path

        return flagstat_cmd
