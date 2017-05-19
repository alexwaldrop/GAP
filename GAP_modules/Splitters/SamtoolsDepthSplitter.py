from GAP_interfaces import Splitter

__main_class__ = "SamtoolsDepthSplitter"

class SamtoolsDepthSplitter(Splitter):

    def __init__(self, config, sample_data, tool_id, main_module_name=None):
        super(SamtoolsDepthSplitter, self).__init__(config, sample_data, tool_id, main_module_name)

        self.input_keys     = ["bam"]
        self.output_keys    = ["bam", "location"]

        self.req_tools      = []
        self.req_resources  = []

    def get_command(self, **kwargs):

        bam   = kwargs.get("bam",         None)

        chrom_list = self.sample_data["chrom_list"]

        # Setting up the splits
        # Process each chromosome separately and process the rest in one single run
        self.output = list()
        for chrom in chrom_list:
            self.output.append(
                {
                    "bam":                  bam,
                    "location":             chrom,
                }
            )

        # No command needs to be run
        return None
