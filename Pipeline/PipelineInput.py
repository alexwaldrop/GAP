import logging

from Sample import Sample
from ConfigParsers import JSONConfigParser
from IO import PlatformFileSet

class PipelineInput(dict):

    def __init__(self, json_input_file):

        # Parse JSON sample sheet and initialize object as a dictionary from config file
        json_parser     = JSONConfigParser(json_input_file, config_spec_file="resources/config_schemas/InputData.validate")
        self.config     = json_parser.get_validated_config()
        super(PipelineInput, self).__init__(**self.config)

        # Obtain sample data
        self.samples        = self.parse_samples()

        # Generate name variables
        self.pipeline_id    = self.config["pipeline_id"]
        self.sample_name    = self.samples.keys()[0] if len(self.samples) == 1 else "Multisample"
        self.pipeline_name  = "%s-%s" % (self.sample_name, self.pipeline_id)

        # Initialize PipelineFileSet to hold input file info for all samples
        self.files      = self.init_input_files()

    def parse_samples(self):
        # Parses sample information and creates sample objects for each sample record in the JSON
        # Checks to make sure all samples contain the same input file keys

        # Generate sample data
        samples = dict()
        for sample in self.config["samples"]:

            # Create sample object from next sample
            sample_name = sample["name"]

            # Check to make sure no duplicate samples exist
            if sample_name in samples:
                logging.error("Pipeline input JSON contains two or more samples with the name '%s'! Sample names must be unique!" % sample_name)
                raise IOError("Two or more samples have the same name in the JSON input file")

            samples[sample_name] = Sample(sample)

        return samples

    def init_input_files(self):
        files = []
        for sample_name, sample in self.samples.iteritems():
            files.extend(sample.input_data)
        return PlatformFileSet(files)

    def get_input_file_types(self):
        # Return input data keys
        return self.samples.values()[0].get_input_keys()

    def get_input_files(self):
        return self.files

    def get_samples(self):
        return self.samples

    def get_sample_name(self):
        return self.sample_name

    def get_pipeline_name(self):
        return self.pipeline_name
