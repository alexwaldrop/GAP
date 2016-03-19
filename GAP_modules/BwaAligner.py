from GAP_interfaces import Aligner

class BwaAligner(Aligner):
    
    def __init__(self, config):

        Aligner.__init__(self, config)

        self.config = config

        self.input_type     = "fastq"
        self.output_type    = "sam"

        self.from_stdout    = False
        self.to_stdout      = True

        self.R1             = None
        self.R2             = None
        
        self.threads        = -1
        self.mem            = 20

    def getCPURequirement(self):
        return self.threads

    def getMemRequirement(self):
        return self.mem

    def getCommand(self):

        if self.threads == -1:
            self.error("In aligner implementation, number of threads not specified")

        return "%s mem -M -t %d %s %s %s" % (self.config.paths.bwa, self.threads, self.config.aligner.ref, self.R1, self.R2)
