from GAP_interfaces import Main

class SamtoolsBAMMerge(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.samtools_path= config.paths.samtools
        self.temp_dir     = config.general.temp_dir

        self.sorted_input = False
        self.nr_splits    = 0
        self.threads      = -1

    def getCommand(self):

        if self.threads == -1:
            self.error("In merger implementation, the number of threads is not specified!")

        if self.sorted_input:
            bam_splits = ["%s/bam_%04d.bam" % (self.temp_dir, i) for i in range(self.nr_splits)]
            return "%s merge -@%d %s/out.bam %s" % (self.samtools_path, self.threads, self.temp_dir, " ".join(bam_splits))
        else:
            bam_splits = ["%s/bam_%04d" % (self.temp_dir, i) for i in range(self.nr_splits)]
            return "%s cat -o %s/out.bam %s" % (self.samtools_path, self.temp_dir, " ".join(bam_splits))

    def validate(self):
        if self.nr_splits == 0:
            self.error("Number of splits was not set before merging!")
