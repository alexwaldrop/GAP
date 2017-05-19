from GAP_interfaces import Tool

__main_class__ = "PicardInsertSizeMetrics"

class PicardInsertSizeMetrics(Tool):

    def __init__(self, config, sample_data, tool_id):
        super(PicardInsertSizeMetrics, self).__init__(config, sample_data, tool_id)

        self.can_split      = False

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        self.input_keys     = ["bam"]
        self.output_keys    = ["insert_size_report", "insert_size_histogram"]

        self.req_tools      = ["picard", "java"]
        self.req_resources  = []

        self.num_reads      = 1000000 #self.config["general"]["num_reads_picard_insert_size"]
        # TODO add to config to specify num reads

    def get_command(self, **kwargs):

        # Obtaining the arguments
        bam    = kwargs.get("bam", None)
        mem    = kwargs.get("mem", self.mem)

        # Generate cmd to run picard insert size metrics
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, self.tmp_dir)
        cmd = "%s %s -jar %s CollectInsertSizeMetrics HISTOGRAM_FILE=%s INPUT=%s OUTPUT=%s STOP_AFTER=%d !LOG2!" \
                     % (self.tools["java"], jvm_options, self.tools["picard"],
                        self.output["insert_size_histogram"], bam,
                        self.output["insert_size_report"],
                        self.num_reads)

        return cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("insert_size_histogram", "insert_size_histogram.pdf")
        self.generate_output_file_path("insert_size_report",    "insertsize.out")
