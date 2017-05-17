from GAP_interfaces import Splitter

__main_class__ = "GATKReferenceSplitter"

class GATKReferenceSplitter(Splitter):

    def __init__(self, config, sample_data):
        super(GATKReferenceSplitter, self).__init__(config, sample_data)

        self.input_keys     = ["bam"]
        self.output_keys    = ["bam", "BQSR_report", "location", "excluded_location"]

        self.req_tools      = []
        self.req_resources  = []

    def get_command(self, **kwargs):

        bam            = kwargs.get("bam",         None)
        BQSR_report    = kwargs.get("BQSR_report", None)

        chrom_list = self.sample_data["chrom_list"]

        # Setting up the splits
        # Process each chromosome separately and process the rest in one single run
        self.output = list()
        for chrom in chrom_list:
            self.output.append(
                {
                    "bam":                  bam,
                    "BQSR_report":          BQSR_report,
                    "location":             chrom,
                    "excluded_location":    None
                }
            )
        self.output.append(
            {
                "bam":                  bam,
                "BQSR_report":          BQSR_report,
                "location":             None,
                "excluded_location":    chrom_list
            }
        )

        # No command needs to be run
        return None
