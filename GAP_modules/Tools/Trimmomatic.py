from GAP_interfaces import Tool

__main_class__ = "Trimmomatic"

class Trimmomatic(Tool):

    def __init__(self, platform, tool_id):
        super(Trimmomatic, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        self.input_keys     = ["R1", "R2"]
        self.output_keys    = ["R1", "R1_unpair", "R2", "R2_unpair", "trim_report"]

        self.req_tools      = ["trimmomatic", "java"]
        self.req_resources  = ["adapters"]

        self.is_phred33     = self.config["sample"]["phred33"]

    def get_command(self, **kwargs):

        # Obtaining the arguments
        R1                 = kwargs.get("R1",              None)
        R2                 = kwargs.get("R2",              None)
        nr_cpus            = kwargs.get("nr_cpus",         self.nr_cpus)
        mem                = kwargs.get("mem",             self.mem)

        steps       = [ "ILLUMINACLIP:%s:2:20:7:1:true" % self.resources["adapters"],
                        "LEADING:5",
                        "TRAILING:5",
                        "SLIDINGWINDOW:4:10",
                        "MINLEN:36" ]

        phred       = "-phred33" if self.is_phred33 else "-phred64"

        jvm_options = "-Xmx%dG -Djava.io.tmp=%s" % (mem*4/5, self.tmp_dir)

        # Generating command
        trim_cmd = "%s %s -jar %s PE -threads %d %s %s %s %s %s %s %s %s > %s 2>&1" % (
            self.tools["java"], jvm_options, self.tools["trimmomatic"], nr_cpus, phred, R1, R2,
            self.output["R1"],
            self.output["R1_unpair"],
            self.output["R2"],
            self.output["R2_unpair"],
            " ".join(steps),
            self.output["trim_report"])

        return trim_cmd

    def init_output_file_paths(self, **kwargs):

        self.generate_output_file_path("R1", "R1.trimmed.fastq")
        self.generate_output_file_path("R1_unpair", "R1.trimmed_unpaired.fastq")
        self.generate_output_file_path("R2", "R2.trimmed.fastq")
        self.generate_output_file_path("R2_unpair", "R2.trimmed_unpaired.fastq")
        self.generate_output_file_path("trim_report", ".trim_report.txt")