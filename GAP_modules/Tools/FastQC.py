from GAP_interfaces import Tool

__main_class__= "FastQC"

class FastQC(Tool):
    
    def __init__(self, config, sample_data):
        super(FastQC, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.fastqc         = self.config["paths"]["fastqc"]
        self.java           = self.config["paths"]["java"]

        self.temp_dir       = self.config["general"]["temp_dir"]

        self.can_split      = False

        self.R1             = None
        self.R2             = None
        self.threads        = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1                 = kwargs.get("R1",              self.sample_data["R1"])
        self.R2                 = kwargs.get("R2",              self.sample_data["R2"])
        self.threads            = kwargs.get("cpus",            self.config["instance"]["nr_cpus"])

        # Generating quality check command
        fastqc_cmd = "%s -t %d --java %s --nogroup %s %s !LOG3!" % (self.fastqc, self.threads, self.java, self.R1, self.R2)

        # Generating the output paths
        self.output = list()
        for fastq_file in [self.R1, self.R2]:
            fastq_filename = fastq_file.split("/")[-1].replace(".fastq.gz", "").replace(".fastq", "")
            self.output.append("%s/%s_fastqc.html" % (self.temp_dir, fastq_filename))
            self.output.append("%s/%s_fastqc.zip" % (self.temp_dir, fastq_filename))

        self.final_output = list(self.output)

        return fastqc_cmd
