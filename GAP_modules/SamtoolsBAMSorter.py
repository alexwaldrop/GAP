from GAP_interfaces import Sorter

class SamtoolsBAMSorter(Sorter):
    
    def __init__(self, config):
        Converter.__init__(self, config)

        self.samtools_path  = config.path.samtools
        
        self.input_type     = "bam"
        self.output_type    = "bam"

        self.from_stdout    = True
        self.to_stdout      = False

        self.threads        = -1
        self.out_prefix     = "split_sorted_"

    def getCommand(self):
        if self.threads  == -1:
            self.error("In sorter implementation, number of threads not specified!")

        return "%s sort -@ %d - %s" % (self.samtools_path, self.threads, self.out_prefix) 
