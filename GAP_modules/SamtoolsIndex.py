import logging

__main_class__ = "SamtoolsIndex"

class SamtoolsIndex(object):

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
            logging.error("BAM index could not be obtained as no bam was obtained.")
            return
        else:
            self.bam                = kwargs.get("bam",              self.sample_data["bam"])

        # Generating indexing command
        index_cmd = "%s index %s" % (self.samtools, self.bam)

        # Generating the output paths
        self.sample_data["bam_index"] = "%s.bai" % self.bam
        self.output_path = []

        return index_cmd
