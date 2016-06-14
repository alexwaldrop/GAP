from GAP_interfaces import Aligner

from GAP_modules import SamtoolsSamToBam as ConverterSamToBam
from GAP_modules import SamtoolsBAMSorter as BAMSorter

__main_class__= "BwaAligner"

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
        
        self.threads        = 1
        self.mem            = 20

        self.sam_to_bam     = ConverterSamToBam(config)
        self.bam_sort       = BAMSorter(config)

        self.can_split      = True
        self.splitter       = "FASTQSplitter"
        self.merger         = "SamtoolsBAMMerge"

    def getCPURequirement(self):
        return self.threads

    def getMemRequirement(self):
        return self.mem

    def getCommand(self):

        if self.threads == -1:
            self.error("In aligner implementation, number of threads not specified")

        aligner_cmd     = "%s mem -M -t %d %s %s %s" % (self.config.paths.bwa, self.threads, self.config.aligner.ref, self.R1, self.R2)

        self.sam_to_bam.threads = self.threads
        sam_to_bam_cmd  = self.sam_to_bam.getCommand()

        self.bam_sort.threads   = self.threads
        self.bam_sort.prefix    = "out"
        bam_sort_cmd    = self.bam_sort.getCommand()

        return "%s | %s | %s" % (aligner_cmd, sam_to_bam_cmd, bam_sort_cmd)
