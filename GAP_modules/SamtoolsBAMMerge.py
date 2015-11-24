from GAP_interfaces import Main

class SamtoolsBAMMerge(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.samtools_path= config.paths.samtools
        self.temp_dir     = config.general.temp_dir

        self.sorted_input = False
        self.nr_splits    = 0

    def getCommand(self):
        bam_splits = ["%s/bam_%04d" % (self.temp_dir, i) for i in range(self.nr_splits)]
        
        if self.sorted_input:
            return "%s merge %s/out.bam %s" % (self.samtools_path, self.temp_dir, " ".join(bam_splits))
        else:
            return "%s cat -o %s/out.bam %s" % (self.samtools_path, self.temp_dir, " ".join(bam_splits))

    def validate(self):
        if self.nr_splits == 0:
            self.error("Number of splits was not set before merging!")
