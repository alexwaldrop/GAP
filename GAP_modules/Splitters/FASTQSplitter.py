import logging
from GAP_interfaces import Splitter

__main_class__ = "FASTQSplitter"

class FASTQSplitter(Splitter):

    def __init__(self, config, sample_data):
        super(FASTQSplitter, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.temp_dir = self.config["general"]["temp_dir"]

        self.prefix = ["fastq_R1", "fastq_R2"]

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

        # Generating the commands
        cmds = list()

        # Computing the number of lines to be split for each file
        logging.info("Counting the number of reads in the FASTQ files.")
        reads_per_split = self.get_nr_reads() / self.nr_splits + 1 # Increment with 1 for integrity after int division
        nr_lines_per_split = reads_per_split * 4

        # Setting up the output paths
        self.splits = [{"R1": "%s/%s_%02d" % (self.temp_dir, self.prefix[0], i),
                        "R2": "%s/%s_%02d" % (self.temp_dir, self.prefix[1], i)} for i in xrange(self.nr_splits)]

        # Splitting the files
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

        return " && ".join(cmds)
