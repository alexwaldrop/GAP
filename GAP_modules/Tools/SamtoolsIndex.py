from GAP_interfaces import Tool

__main_class__ = "SamtoolsIndex"

class SamtoolsIndex(Tool):

    def __init__(self, platform, tool_id):
        super(SamtoolsIndex, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        self.input_keys     = ["bam"]
        self.output_keys    = ["bam_idx"]

        self.req_tools      = ["samtools"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        bam = kwargs.get("bam", None)

        # Generating indexing command
        index_cmd = "%s index %s %s" % (self.tools["samtools"], bam, self.output["bam_idx"])

        return index_cmd

    def init_output_file_paths(self, **kwargs):
        bam = kwargs.get("bam", None)
        self.declare_output_file_path("bam_idx", "%s.bai" % bam)
