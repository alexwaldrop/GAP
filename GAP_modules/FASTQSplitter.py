from GAP_interfaces import Main
import os
import math

class FASTQSplitter(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.temp_dir = config.general.temp_dir

    def byNrReads(self, file_path, type, nr_reads):

        # Setting the required values in the object
        self.file_path  = file_path
        self.type       = type
        self.nr_reads   = nr_reads
        with open(file_path) as inp:
            self.total_reads= sum(1 for line in inp) / 4

        # Validating the values
        self.validate()

        # Computing the number of splits
        self.split_count = int(math.ceil(float(self.total_reads) / self.nr_reads))

        # Setting up the prefix and suffix of the splits
        if self.type == "PE_R1":
            split_prefix  = "fastq_R1_"
        elif self.type == "PE_R2":
            split_prefix  = "fastq_R2_"
        elif self.type == "SE":
            split_prefix  = "fastq_"
        else:
            self.warning("Unrecognized FASTQ file type '%s' in the pipeline. Default: Single-End.") 
            split_prefix  = "fastq_"

        # Returning the command to be run
        return "split --suffix-length=4 --numeric-suffixes --lines=%d %s %s/%s" % (nr_reads*4, file_path, self.temp_dir, split_prefix)

    def validate(self):
        
        if not os.path.isfile(self.file_path):
            self.error("Input file could not be found!")

        if self.nr_reads <= 0:
            self.error("Cannot split a FASTQ file by %d reads!" % self.nr_reads)
