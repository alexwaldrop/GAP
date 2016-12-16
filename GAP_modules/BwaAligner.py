__main_class__= "BwaAligner"

class BwaAligner(object):
    
    def __init__(self, config, sample_data):

        self.config = config
        self.sample_data = sample_data

        self.bwa            = self.config["paths"]["bwa"]
        self.samtools       = self.config["paths"]["samtools"]
        self.ref            = self.config["paths"]["ref"]

        self.temp_dir       = self.config["general"]["temp_dir"]

        self.sample_name    = self.sample_data["sample_name"]

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

        # Generating command for alignment
        aligner_cmd = "%s mem -M -t %d %s %s %s" % (self.bwa, self.threads, self.ref, self.R1, self.R2)

        # Generating command for converting SAM to BAM
        sam_to_bam_cmd  = "%s view -bS -@ %d -" % (self.samtools, self.threads)

        # Generating command for sorting BAM
        self.output_prefix = self.sample_name
        if self.split_id is not None:
            self.output_prefix += "_%d" % self.split_id
        bam_sort_cmd = "%s sort -@ %d - %s/%s" % (self.samtools, self.threads, self.temp_dir, self.output_prefix)

        # Generating the output path
        self.output_path = "%s/%s.bam" % (self.temp_dir, self.output_prefix)

        return "%s | %s | %s" % (aligner_cmd, sam_to_bam_cmd, bam_sort_cmd)
