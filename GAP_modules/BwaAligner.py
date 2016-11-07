from GAP_interfaces import Aligner

from GAP_modules import SamtoolsSamToBam as ConverterSamToBam
from GAP_modules import SamtoolsBAMSorter as BAMSorter

__main_class__= "BwaAligner"

class BwaAligner(Aligner):
    
    def __init__(self, config, sample_data):

        Aligner.__init__(self, config)

        self.config = config
        self.sample_data = sample_data

        self.bwa            = self.config["paths"]["bwa"]
        self.ref            = self.config["paths"]["ref"]

        self.sample_name    = self.sample_data["sample_name"]

        self.sam_to_bam     = ConverterSamToBam(config)
        self.bam_sort       = BAMSorter(config)

        self.can_split      = True
        self.splitter       = "FASTQSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.R1             = None
        self.R2             = None
        self.threads        = None
        self.split_id       = None
        self.output_path    = None

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1                 = kwargs.get("R1",              self.sample_data["R1"])
        self.R2                 = kwargs.get("R2",              self.sample_data["R2"])
        self.threads            = kwargs.get("cpus",            self.config["instance"]["nr_cpus"])
        self.split_id           = kwargs.get("split_id",        None)

        self.validate()

        aligner_cmd = "%s mem -M -t %d %s %s %s" % (self.bwa, self.threads, self.ref, self.R1, self.R2)

        sam_to_bam_cmd  = self.sam_to_bam.get_command()

        if self.split_id is None:
            bam_sort_cmd = self.bam_sort.get_command( prefix=self.sample_name )
        else:
            bam_sort_cmd = self.bam_sort.get_command( prefix="%s_%d" % (self.sample_name, self.split_id) )
        self.output_path = self.bam_sort.get_output()

        return "%s | %s | %s" % (aligner_cmd, sam_to_bam_cmd, bam_sort_cmd)

    def validate(self):
        if self.threads == -1:
            self.error("In aligner implementation, number of threads not specified")
