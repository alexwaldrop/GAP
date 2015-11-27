from GAP_interfaces import Main
import os
import math

class FASTQSplitter(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.temp_dir = config.general.temp_dir

    def byNrReads(self, nr_reads, R1_file, R2_file=None):

        # Setting the required values in the object
        self.R1         = R1_file
        self.R2         = R2_file
        self.nr_reads   = nr_reads
        with open(R1_file) as inp:
            self.total_reads= sum(1 for line in inp) / 4

        # Validating the values
        self.validate()

        # Computing the number of splits
        self.split_count = int(math.ceil(float(self.total_reads) / self.nr_reads))

        # Setting up the prefix of the splits
        if R2_file != None:
            # Pair-End Sequencing
            R1_pre  = "fastq_R1_"
            R2_pre  = "fastq_R2_"
        else:
            # Single-End Sequencing
            R1_pre  = "fastq_"

        # Generating the required commands
        R1_command = "split --suffix-length=4 --numeric-suffixes --lines=%d %s %s/%s" % (self.nr_reads*4, self.R1, self.temp_dir, R1_pre)
        if R2_file != None:
            R2_command = "split --suffix-length=4 --numeric-suffixes --lines=%d %s %s/%s" % (self.nr_reads*4, self.R2, self.temp_dir, R2_pre)

        # Returning the command to be run
        if R2_file != None:
            return "%s && %s" % (R1_command, R2_command)
        else:
            return R1_command

    def validate(self):
        
        if not os.path.isfile(self.R1):
            self.error("Input file(s) could not be found!")

        if self.R2 != None:
            if not os.path.isfile(self.R2):
                self.error("Input file(s) could not be found!")

        if self.nr_reads <= 0:
            self.error("Cannot split a FASTQ file by %d reads!" % self.nr_reads)
