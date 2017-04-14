import logging
import math

from GAP_interfaces import Splitter

__main_class__ = "BwaFastqSplitter"

class BwaFastqSplitter(Splitter):

    def __init__(self, config, sample_data):
        super(BwaFastqSplitter, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.temp_dir = self.config["general"]["temp_dir"]

        self.prefix = ["fastq_R1", "fastq_R2"]

        # BWA-MEM aligning speed
        self.ALIGN_SPEED = 10**8 # bps/vCPU for 10 mins of processing
        self.READ_LENGTH = self.sample_data["read_length"]
        self.MAX_NR_CPUS = self.config["platform"]["max_nr_cpus"]

        self.nr_cpus     = self.config["platform"]["MS_nr_cpus"]
        self.mem         = self.config["platform"]["MS_mem"]

        self.R1          = None
        self.R2          = None

    def get_nr_reads(self):
        # Obtain the number of lines in the FASTQ
        if self.R1.endswith(".gz"):
            cmd = "pigz -p %d -d -k -c %s | wc -l" % (self.nr_cpus, self.R1)
        else:
            cmd = "cat %s | wc -l" % self.R1
        out, err = self.sample_data["main-server"].run_command("fastq_count", cmd, log=False, get_output=True)
        if err != "":
            err_msg = "Could not obtain the number of reads in the FASTQ file. "
            err_msg += "\nThe following command was run: \n  %s " % cmd
            err_msg += "\nThe following error appeared: \n  %s" % err
            logging.error(err_msg)
            exit(1)

        self.sample_data["nr_reads"] = int(out) / 4

        return self.sample_data["nr_reads"]

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1             = kwargs.get("R1",              self.sample_data["R1"])
        self.R2             = kwargs.get("R2",              self.sample_data["R2"])
        self.nr_cpus        = kwargs.get("nr_cpus",         self.nr_cpus)
        self.mem            = kwargs.get("mem",             self.mem)

        # Identifying the total number of reads
        logging.info("Counting the number of reads in the FASTQ files.")
        nr_reads = self.get_nr_reads()

        # Computing the number of lines to be split for each file considering:
        #  - The aligning speed which is in bps/vCPU
        #  - The maximum number of vCPUs alloed on the platform
        #  - The difference between read and read pair (divide by 2)
        nr_reads_per_split = self.ALIGN_SPEED / self.READ_LENGTH / 2 * self.MAX_NR_CPUS
        nr_lines_per_split = nr_reads_per_split * 4
        nr_splits = int(math.ceil(nr_reads * 1.0 / nr_reads_per_split))

        # Generating the commands for splitting
        cmds = list()
        for prefix in self.prefix:
            if "R1" in prefix:
                if self.R1.endswith(".gz"):
                    cmd = "pigz -p %d -d -k -c %s | split --suffix-length=2 --numeric-suffixes --lines=%d - %s/%s_" % (self.nr_cpus, self.R1, nr_lines_per_split, self.temp_dir, prefix)
                else:
                    cmd = "split --suffix-length=2 --numeric-suffixes --lines=%d %s %s/%s_" % (nr_lines_per_split, self.R1, self.temp_dir, prefix)
            else:
                if self.R2.endswith(".gz"):
                    cmd = "pigz -p %d -d -k -c %s | split --suffix-length=2 --numeric-suffixes --lines=%d - %s/%s_" % (self.nr_cpus, self.R2, nr_lines_per_split, self.temp_dir, prefix)
                else:
                    cmd = "split --suffix-length=2 --numeric-suffixes --lines=%d %s %s/%s_" % (nr_lines_per_split, self.R2, self.temp_dir, prefix)
            cmds.append(cmd)

        # Identifying the total number of vCPUs needed
        nr_cpus_needed = int(math.ceil(nr_reads * self.READ_LENGTH * 2 * 1.0 / self.ALIGN_SPEED))

        # Preparing the splits. All splits will require MAX_NR_CPUS, except the last one
        self.splits = list()
        for split_id in xrange(nr_splits-1):
            self.splits.append(
                {
                    "R1" : "%s/%s_%02d" % (self.temp_dir, self.prefix[0], split_id),
                    "R2" : "%s/%s_%02d" % (self.temp_dir, self.prefix[1], split_id),
                    "nr_cpus": self.MAX_NR_CPUS
                }
            )

        # Adding the last split
        nr_cpus_remaining = nr_cpus_needed % self.MAX_NR_CPUS
        nr_cpus_remaining += nr_cpus_remaining % 2
        self.splits.append(
            {
                "R1": "%s/%s_%02d" % (self.temp_dir, self.prefix[0], nr_splits-1),
                "R2": "%s/%s_%02d" % (self.temp_dir, self.prefix[1], nr_splits-1),
                "nr_cpus": nr_cpus_remaining
            }
        )

        return " && ".join(cmds)
