from GAP_interfaces import Splitter

__main_class__ = "GATKReferenceSplitter"

class GATKReferenceSplitter(Splitter):

    def __init__(self, config, sample_data):
        super(GATKReferenceSplitter, self).__init__()

        self.config = config
        self.sample_data = sample_data

    def get_command(self, **kwargs):

        chrom_list = self.sample_data["chrom_list"]

        # Setting up the splits
        # Process each chromosome separately and process the rest in one single run
        self.splits = list()
        for chrom in chrom_list:
            self.splits.append(
                {
                    "location": chrom,
                    "excluded_location": None
                }
            )
        self.splits.append(
            {
                "location": None,
                "excluded_location": chrom_list
            }
        )

        # No command needs to be run
        return None
