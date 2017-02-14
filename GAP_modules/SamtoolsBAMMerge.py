import hashlib
import time
import logging

__main_class__ = "SamtoolsBAMMerge"

class SamtoolsBAMMerge(object):

    def __init__(self, config, sample_data):

        self.config = config
        self.sample_data = sample_data

        self.samtools     = self.config["paths"]["samtools"]

        self.temp_dir     = self.config["general"]["temp_dir"]

        self.sample_name  = self.sample_data["sample_name"]

        self.threads      = None
        self.inputs       = None
        self.nr_splits    = None
        self.sorted_input = None
        self.output_path  = None

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.threads        = kwargs.get("cpus",            self.config["instance"]["nr_cpus"])
        self.inputs         = kwargs.get("inputs",          None)
        self.nr_splits      = kwargs.get("nr_splits",       2)
        self.sorted_input   = kwargs.get("sorted_input",    True)

        if self.inputs is None:
            logging.error("Cannot merge as no inputs were received. Check if the previous module does return the bam paths to merge.")

        # Generating the output path
        self.output_path = "%s/%s_%s.bam" % (self.temp_dir, self.sample_name, hashlib.md5(str(time.time())).hexdigest()[:5])
        self.sample_data["bam"] = self.output_path

        # Generating the merging command
        if self.sorted_input:
            bam_merge_cmd = "%s merge -@%d %s %s" % (self.samtools, self.threads, self.output_path, " ".join(self.inputs))
        else:
            bam_merge_cmd = "%s cat -o %s %s" % (self.samtools, self.output_path, " ".join(self.inputs))

        return bam_merge_cmd