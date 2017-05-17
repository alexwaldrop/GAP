from GAP_interfaces import Tool

__main_class__= "FastQC"

class FastQC(Tool):
    
    def __init__(self, config, sample_data):
        super(FastQC, self).__init__(config, sample_data)

        self.temp_dir       = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = False

        self.nr_cpus        = 2     # FASTQC requires 1 CPU per FASTQ file
        self.mem            = 5     # FASTQC requires 250MB RAM per FASTQ file

        self.input_keys     = ["R1", "R2"]
        self.output_keys    = ["R1_html", "R1_zip", "R2_html", "R2_zip"]

        self.req_tools      = ["fastqc", "java"]
        self.req_resources  = []

        self.R1             = None
        self.R2             = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1                 = kwargs.get("R1",              None)
        self.R2                 = kwargs.get("R2",              None)
        self.nr_cpus            = kwargs.get("nr_cpus",         self.nr_cpus)
        self.mem                = kwargs.get("mem",             self.mem)

        # Generating quality check command
        fastqc_cmd = "%s -t %d --java %s --nogroup %s %s !LOG3!" % (self.tools["fastqc"], self.nr_cpus, self.tools["java"], self.R1, self.R2)

        # Generating the output paths
        self.output = dict()
        self.output["R1_html"]  = "%s_fastqc.html" % self.R1.replace(".fastq.gz", "").replace(".fastq", "")
        self.output["R1_zip"]   = "%s_fastqc.zip"  % self.R1.replace(".fastq.gz", "").replace(".fastq", "")
        self.output["R2_html"]  = "%s_fastqc.html" % self.R2.replace(".fastq.gz", "").replace(".fastq", "")
        self.output["R2_zip"]   = "%s_fastqc.zip"  % self.R2.replace(".fastq.gz", "").replace(".fastq", "")

        return fastqc_cmd
