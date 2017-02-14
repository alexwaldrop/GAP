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
        self.pipeline_output_path = None

    def get_pipeline_output(self):
        return self.pipeline_output_path

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
        sam_to_bam_cmd  = "%s view -uS -@ %d -" % (self.samtools, self.threads)

        # Generating the output path
        self.output_path = "%s/%s" % (self.temp_dir, self.sample_name)
        if self.split_id is not None:
            self.output_path += "_%d.bam" % self.split_id
        else:
            self.output_path += ".bam"
            self.sample_data["bam"] = self.output_path

        # Generating command for sorting BAM
        bam_sort_cmd = "%s sort -@ %d - -o %s" % (self.samtools, self.threads, self.output_path)

        return "%s | %s | %s" % (aligner_cmd, sam_to_bam_cmd, bam_sort_cmd)
