from GAP_interfaces import Tool

__main_class__ = "SamtoolsFlagstat"

class SamtoolsFlagstat(Tool):

    def __init__(self, config, sample_data):
        super(SamtoolsFlagstat, self).__init__(config, sample_data)

        self.can_split      = False

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        self.input_keys     = ["bam"]
        self.output_keys    = ["flagstat"]

        self.req_tools      = ["samtools"]
        self.req_resources  = ["ref"]

    def get_command(self, **kwargs):

        bam                    = kwargs.get("bam",              None)

        bam_prefix = bam.split(".")[0]

        # Generating indexing command
        flagstat_cmd = "%s flagstat %s > %s_flagstat.txt" % (self.tools["samtools"], bam, bam_prefix)

        # Generating the output
        self.output = dict()
        self.output["flagstat"] = "%s_flagstat.txt" % bam_prefix

        return flagstat_cmd
