import logging

__main_class__ = "FASTQSplitter"

class FASTQSplitter(object):

    def __init__(self, config, sample_data):

        self.config = config
        self.sample_data = sample_data

        self.temp_dir = self.config["general"]["temp_dir"]

        self.prefix = ["fastq_R1", "fastq_R2"]

        self.R1          = None
        self.R2          = None
        self.nr_splits   = None
        self.output_path = None

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1             = kwargs.get("R1",              self.sample_data["R1"])
        self.R2             = kwargs.get("R2",              self.sample_data["R2"])
        self.threads        = kwargs.get("cpus",            self.config["instance"]["nr_cpus"])
        self.nr_splits      = kwargs.get("nr_splits",       self.config["general"]["nr_splits"])

        # Generating the commands
        cmds = list()

        # Integer division and multiplication to ensure reads integrity
        # Increment with 1, so that it includes the remaining reads (remainder after integer division)
        logging.info("Counting the number of reads in the FASTQ files.")
        if self.R1.endswith(".gz"):
            cmds.append("nr_lines=$(( (`pigz -p %d -d -k -c %s | wc -l` / (%d * 4) + 1) * 4 ))" % (self.threads, self.R1, self.nr_splits) )
        else:
            cmds.append("nr_lines=$(( (`cat %s | wc -l` / (%d * 4) + 1) * 4 ))" % (self.R1, self.nr_splits) )

        # Setting up the output paths
        self.output_path = [ { "R1" : "%s/%s_%02d" % (self.temp_dir, self.prefix[0], i),
                               "R2" : "%s/%s_%02d" % (self.temp_dir, self.prefix[1], i) } for i in xrange(self.nr_splits)]

        # Splitting the files
        for prefix in self.prefix:
            if "R1" in prefix:
                if self.R1.endswith(".gz"):
                    cmd = "pigz -p %d -d -k -c %s | split --suffix-length=2 --numeric-suffixes --lines=$nr_lines - %s/%s_" % (self.threads, self.R1, self.temp_dir, prefix)
                else:
                    cmd = "split --suffix-length=2 --numeric-suffixes --lines=$nr_lines %s %s/%s_" % (self.R1, self.temp_dir, prefix)
            else:
                if self.R2.endswith(".gz"):
                    cmd = "pigz -p %d -d -k -c %s | split --suffix-length=2 --numeric-suffixes --lines=$nr_lines - %s/%s_" % (self.threads, self.R2, self.temp_dir, prefix)
                else:
                    cmd = "split --suffix-length=2 --numeric-suffixes --lines=$nr_lines %s %s/%s_" % (self.R2, self.temp_dir, prefix)
            cmds.append(cmd)

        return " && ".join(cmds)
