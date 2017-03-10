__main_class__ = "GATKReferenceSplitter"

class GATKReferenceSplitter(object):

    def __init__(self, config, sample_data):
        self.config = config
        self.sample_data = sample_data

        self.output_path = None
        self.pipeline_output_path = None

    def get_pipeline_output(self):
        return self.pipeline_output_path

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        chrom_list = self.sample_data["chrom_list"]

        # Setting up the output paths
        # Process each chromosome separately and process the rest in one single run
        self.output_path = [ {"location": chrom, "excluded_location": None} for chrom in chrom_list ]
        self.output_path.append( {"location": None, "excluded_location": chrom_list} )

        # No command needs to be run
        return None
