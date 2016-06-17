import os

from GAP_interfaces import Main

__main_class__ = "FASTQSplitter"

class FASTQSplitter(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.temp_dir = config.general.temp_dir

        self.R1 = None
        self.R2 = None

        self.nr_splits = 2

    def getCommand(self):

        # Generating the commands
        cmds = []

        # Obtaining number of reads
        cmds.append("nr_lines=$(( `du -b %s | cut -f1` / `head -n4 %s | wc -c` / %d * 4))" % (self.R1, self.R1, self.nr_splits) )

        # Splitting the files
        cmds.append("split --suffix-length=2 --numeric-suffixes --lines=$nr_lines %s %s/fastq_R1_" % (self.R1, self.temp_dir) )
        cmds.append("split --suffix-length=2 --numeric-suffixes --lines=$nr_lines %s %s/fastq_R2_" % (self.R2, self.temp_dir) )

        return " && ".join(cmds)

    def validate(self):

        pass
