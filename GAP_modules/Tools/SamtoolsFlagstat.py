from GAP_interfaces import Tool

__main_class__ = "SamtoolsFlagstat"

class SamtoolsFlagstat(Tool):

    def __init__(self, config, sample_data, tool_id):
        super(SamtoolsFlagstat, self).__init__(config, sample_data, tool_id)

        self.can_split      = False

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        self.input_keys     = ["bam"]
        self.output_keys    = ["flagstat"]

        self.req_tools      = ["samtools"]
        self.req_resources  = ["ref"]

    def get_command(self, **kwargs):

        bam = kwargs.get("bam", None)

        # Generating flagstat command
        cmd = "%s flagstat %s > %s" % (self.tools["samtools"], bam, self.output["flagstat"])

        return cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("flagstat", "flagstat.out")
