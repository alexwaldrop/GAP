from GAP_interfaces import Tool

__main_class__ = "PicardInsertSizeMetrics"

class PicardInsertSizeMetrics(Tool):

    def __init__(self, config, sample_data):
        super(PicardInsertSizeMetrics, self).__init__(config, sample_data)

        self.temp_dir       = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = False

        self.nr_cpus        = self.config["platform"]["MS_nr_cpus"]
        self.mem            = self.config["platform"]["MS_mem"]

        self.num_reads      = 1000000 #self.config["general"]["num_reads_picard_insert_size"]
        # TODO add to config to specify num reads

        self.input_keys     = ["bam"]
        self.output_keys    = ["insert_size_report", "insert_size_histogram"]

        self.req_tools      = ["picard", "java"]
        self.req_resources  = []

        self.bam            = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.bam    = kwargs.get("bam", None)
        self.mem    = kwargs.get("mem", self.mem)

        # Generate output filenames
        bam_prefix = self.bam.split(".")[0]
        histogram_output = "%s_insert_size_histogram.pdf" % bam_prefix
        text_output      = "%s_insert_size.txt" % bam_prefix

        # Generate cmd to run picard insert size metrics
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (self.mem * 4 / 5, self.temp_dir)
        cmd = "%s %s -jar %s CollectInsertSizeMetrics HISTOGRAM_FILE=%s INPUT=%s OUTPUT=%s STOP_AFTER=%d !LOG2!" \
                     % (self.tools["java"], jvm_options, self.tools["picard"], histogram_output, self.bam, text_output, self.num_reads)

        # Generating the output
        self.output = dict()
        self.output["insert_size_report"]       = text_output
        self.output["insert_size_histogram"]    = histogram_output

        return cmd
