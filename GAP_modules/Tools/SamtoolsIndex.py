from GAP_interfaces import Tool

__main_class__ = "SamtoolsIndex"

class SamtoolsIndex(Tool):

    def __init__(self, config, sample_data):
        super(SamtoolsIndex, self).__init__(config, sample_data)

        self.can_split      = False

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        self.input_keys     = ["bam"]
        self.output_keys    = ["bam_idx"]

        self.req_tools      = ["samtools"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        bam                    = kwargs.get("bam",              None)

        # Generate index name
        bam_prefix = bam.split(".")[0]
        bam_index = "%s.bai" % bam_prefix

        # Generating indexing command
        index_cmd = "%s index %s %s" % (self.tools["samtools"], bam, bam_index)

        # Generating the output paths
        self.output = dict()
        self.output["bam_idx"] = bam_index

        return index_cmd
