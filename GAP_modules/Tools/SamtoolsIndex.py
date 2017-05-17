from GAP_interfaces import Tool

__main_class__ = "SamtoolsIndex"

class SamtoolsIndex(Tool):

    def __init__(self, config, sample_data):
        super(SamtoolsIndex, self).__init__(config, sample_data)

        self.temp_dir       = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = False

        self.nr_cpus        = self.config["platform"]["MS_nr_cpus"]
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["bam"]
        self.output_keys    = ["bam_idx"]

        self.req_tools      = ["samtools"]
        self.req_resources  = []

        self.bam            = None

    def get_command(self, **kwargs):

        self.bam                    = kwargs.get("bam",              None)
        self.nr_cpus                = kwargs.get("nr_cpus",          self.nr_cpus)
        self.mem                    = kwargs.get("mem",              self.mem)

        # Generate index name
        bam_prefix = self.bam.split(".")[0]
        bam_index = "%s.bai" % bam_prefix

        # Generating indexing command
        index_cmd = "%s index %s %s" % (self.tools["samtools"], self.bam, bam_index)

        # Generating the output paths
        self.output = dict()
        self.output["bam_idx"] = bam_index

        return index_cmd
