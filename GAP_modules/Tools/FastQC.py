from GAP_interfaces import Tool

__main_class__= "FastQC"

class FastQC(Tool):
    
    def __init__(self, platform, tool_id):
        super(FastQC, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = 2     # FASTQC requires 1 CPU per FASTQ file
        self.mem            = 5     # FASTQC requires 250MB RAM per FASTQ file

        self.input_keys     = ["R1", "R2"]
        self.output_keys    = ["R1_fastqc", "R2_fastqc"]

        self.req_tools      = ["fastqc", "java"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Obtaining the arguments
        R1                 = kwargs.get("R1",              None)
        R2                 = kwargs.get("R2",              None)
        nr_cpus            = kwargs.get("nr_cpus",         self.nr_cpus)

        # Generating quality check command
        fastqc_cmd = "%s -t %d --java %s --nogroup --extract %s %s !LOG3!" % (self.tools["fastqc"], nr_cpus, self.tools["java"], R1, R2)

        return fastqc_cmd

    def init_output_file_paths(self, **kwargs):

        # Obtaining the arguments
        R1 = kwargs.get("R1", None)
        R2 = kwargs.get("R2", None)

        # Generate output filenames
        self.declare_output_file_path("R1_fastqc", "%s_fastqc" % R1.replace(".fastq.gz", "").replace(".fastq", ""))
        self.declare_output_file_path("R2_fastqc", "%s_fastqc" % R2.replace(".fastq.gz", "").replace(".fastq", ""))
