from GAP_interfaces import Sorter

class SamtoolsBAMSorter(Sorter):
    
    def __init__(self, config):
        Sorter.__init__(self, config)

        self.samtools_path  = config.paths.samtools
        
        self.input_type     = "bam"
        self.output_type    = "bam"

        self.from_stdout    = True
        self.to_stdout      = False

        self.threads        = -1
        self.temp_dir       = config.general.temp_dir
        self.prefix         = ""

    def getCommand(self):
        if self.threads  == -1:
            self.error("In sorter implementation, number of threads not specified!")

        if self.prefix == "":
            self.error("In sorter implementation, the prefix is not specified!")

        return "%s sort -@ %d - %s/%s" % (self.samtools_path, self.threads, self.temp_dir, self.prefix) 
