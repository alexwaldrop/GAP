import logging
import math

from GAP_interfaces import Splitter

__main_class__ = "BwaFastqSplitter"

class BwaFastqSplitter(Splitter):

    def __init__(self, config, sample_data):
        super(BwaFastqSplitter, self).__init__(config, sample_data)

        self.nr_cpus     = self.main_server_nr_cpus
        self.mem         = self.main_server_mem

        self.input_keys  = ["R1", "R2"]
        self.output_keys = ["R1", "R2", "nr_cpus"]

        self.req_tools      = []
        self.req_resources  = []

        # BWA-MEM aligning speed
        self.ALIGN_SPEED = 10**8 # bps/vCPU for 10 mins of processing
        self.READ_LENGTH = self.sample_data["read_length"]
        self.MAX_NR_CPUS = self.max_nr_cpus

        self.prefix = ["fastq_R1", "fastq_R2"]

    def get_nr_reads(self, R1, nr_cpus):
        # Obtain the number of lines in the FASTQ
        if R1.endswith(".gz"):
            cmd = "pigz -p %d -d -k -c %s | wc -l" % (nr_cpus, R1)
        else:
            cmd = "cat %s | wc -l" % R1

        self.sample_data["main-server"].run_command("fastq_count", cmd, log=False)
        out, err = self.sample_data["main-server"].get_proc_output("fastq_count")

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
        R1             = kwargs.get("R1",              None)
        R2             = kwargs.get("R2",              None)
        nr_cpus        = kwargs.get("nr_cpus",         self.nr_cpus)

        # Identifying the total number of reads
        logging.info("Counting the number of reads in the FASTQ files.")
        nr_reads = self.get_nr_reads(R1, nr_cpus)

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
                if R1.endswith(".gz"):
                    cmd = "pigz -p %d -d -k -c %s | split --suffix-length=2 --numeric-suffixes --lines=%d - %s/%s_" % (nr_cpus, R1, nr_lines_per_split, self.tmp_dir, prefix)
                else:
                    cmd = "split --suffix-length=2 --numeric-suffixes --lines=%d %s %s/%s_" % (nr_lines_per_split, R1, self.tmp_dir, prefix)
            else:
                if R2.endswith(".gz"):
                    cmd = "pigz -p %d -d -k -c %s | split --suffix-length=2 --numeric-suffixes --lines=%d - %s/%s_" % (nr_cpus, R2, nr_lines_per_split, self.tmp_dir, prefix)
                else:
                    cmd = "split --suffix-length=2 --numeric-suffixes --lines=%d %s %s/%s_" % (nr_lines_per_split, R2, self.tmp_dir, prefix)
            cmds.append(cmd)

        # Identifying the total number of vCPUs needed
        nr_cpus_needed = int(math.ceil(nr_reads * self.READ_LENGTH * 2 * 1.0 / self.ALIGN_SPEED))

        # Preparing the splits. All splits will require MAX_NR_CPUS, except the last one
        self.output = list()
        for split_id in xrange(nr_splits-1):
            self.output.append(
                {
                    "R1" : "%s/%s_%02d" % (self.tmp_dir, self.prefix[0], split_id),
                    "R2" : "%s/%s_%02d" % (self.tmp_dir, self.prefix[1], split_id),
                    "nr_cpus": self.MAX_NR_CPUS
                }
            )

        # Adding the last split
        nr_cpus_remaining = nr_cpus_needed % self.MAX_NR_CPUS
        nr_cpus_remaining += nr_cpus_remaining % 2
        self.output.append(
            {
                "R1": "%s/%s_%02d" % (self.tmp_dir, self.prefix[0], nr_splits-1),
                "R2": "%s/%s_%02d" % (self.tmp_dir, self.prefix[1], nr_splits-1),
                "nr_cpus": nr_cpus_remaining
            }
        )

        return " && ".join(cmds)
