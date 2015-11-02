from GAP_interfaces import Converter

class SamtoolsSamToBam(Converter):
    
    def __init__(self, config):
        Converter.__init__(self, config)

        self.config = config
        
        self.input_type     = "sam"
        self.output_type    = "bam"

        self.from_stdout    = True
        self.to_stdout      = True

        self.threads        = -1

    def getCommand(self):
        if self.thread  == -1:
            self.error("In converter implementation, number of threads not specified!")

        return "%s view -bS -@ %d -" % (self.config.paths.samtools, self.threads) 
