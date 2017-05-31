from GAP_interfaces import Splitter

__main_class__ = "GATKReferenceSplitter"

class GATKReferenceSplitter(Splitter):

    def __init__(self, platform, tool_id, main_module_name=None):
        super(GATKReferenceSplitter, self).__init__(platform, tool_id, main_module_name)

        self.input_keys     = ["bam"]
        self.output_keys    = ["bam", "BQSR_report", "location", "excluded_location"]

        self.req_tools      = []
        self.req_resources  = []

    def init_split_info(self, **kwargs):
        # Obtain arguments
        bam = kwargs.get("bam", None)
        BQSR_report = kwargs.get("BQSR_report", None)

        # Get information related to each split
        # Process each chromosome separately and process the rest in one single run
        chrom_list = self.config["sample"]["chrom_list"]
        for chrom in chrom_list:
            self.output.append(
                {
                    "bam": bam,
                    "BQSR_report": BQSR_report,
                    "location": chrom,
                    "excluded_location": None
                }
            )
        self.output.append(
            {
                "bam": bam,
                "BQSR_report": BQSR_report,
                "location": None,
                "excluded_location": chrom_list
            }
        )

    def init_output_file_paths(self, **kwargs):
        # No output files need to be generated
        return None

    def get_command(self, **kwargs):
        # No command needs to be run
        return None



