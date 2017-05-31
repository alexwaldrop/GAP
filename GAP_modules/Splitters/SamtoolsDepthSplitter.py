from GAP_interfaces import Splitter

__main_class__ = "SamtoolsDepthSplitter"

class SamtoolsDepthSplitter(Splitter):

    def __init__(self, platform, tool_id, main_module_name=None):
        super(SamtoolsDepthSplitter, self).__init__(platform, tool_id, main_module_name)

        self.input_keys     = ["bam"]
        self.output_keys    = ["bam", "location"]

        self.req_tools      = []
        self.req_resources  = []

    def init_split_info(self, **kwargs):
        # Each split will return a bam file and a chromosome name
        bam = kwargs.get("bam", None)

        chrom_list = self.config["sample"]["chrom_list"]

        # Setting up the splits
        # Process each chromosome separately and process the rest in one single run
        for chrom in chrom_list:
            self.output.append(
                {
                    "bam": bam,
                    "location": chrom,
                }
            )

    def init_output_file_paths(self, **kwargs):
        # No output files need to be generated
        pass

    def get_command(self, **kwargs):
        # No command needs to be run
        return None
