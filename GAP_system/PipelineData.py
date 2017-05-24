import json
import logging
import os

from SampleData import SampleData

class PipelineData:
    def __init__(self, json_input):

        # Takes a JSON file as input
        self.json_input     = json_input

        # Required keys in JSON file
        self.required_keys  = ["samples", "pipeline_id"]

        # Parse JSON input
        self.data           = self.parse_json_input()

        # Check for presence of required keys
        self.check_required_keys()

        # Create unique pipeline id
        self.pipeline_id    = str(self.data["pipeline_id"])

        # Set required attributes
        self.sample_data    = self.get_sample_data()

        # Set unique name for pipeline
        self.pipeline_name  = self.get_pipeline_name()

        # Dictionary for holding global output files produced by pipeline
        self.final_output = dict()

    def get_sample_data(self):
        # Parses sample information and creates sample objects for each sample record in the JSON
        sample_data = dict()
        for sample in self.data["samples"]:
            # Create sample object from next sample
            sample_name = sample["name"]

            # Check to make sure no duplicate samples exist
            if sample_name in sample_data:
                msg = "Pipeline input JSON contains two or more samples with the name '%s'! Sample names must be unique!" % sample_name
                logging.error(msg)
                raise IOError(msg)

            sample_data[sample_name] = SampleData(sample_name, sample["input"])
        return sample_data

    def parse_json_input(self):
        # Parses JSON input file and sets attributes of PipelineData class
        # Checks format of JSON to ensure presence of certain attributes
        # Throws errors if JSON can't be parsed or if JSON doesn't contain necessary information

        # Check to make sure JSON exists
        if not os.path.exists(self.json_input):
            msg = "Could not locate sample JSON input: %s" % self.json_input
            logging.error(msg)
            raise IOError(msg)

        # Load file into a dictionary
        try:
            with open(self.json_input, "r") as ft:
                data = json.load(ft)

        except:
            logging.error("Pipeline data input file is not a valid JSON file: %s." % self.json_input)
            raise

        return data

    def check_required_keys(self):
        # Checks for the presence of specific keys in a JSON file
        errors = False
        for key in self.required_keys:
            if key not in self.data:
                errors = True
                logging.error("Required key '%s' not found in Pipeline JSON input file: %s" % (key, self.json_input))

        if errors:
            raise IOError("One or more required keys was not found in the JSON input file: %s. See above for errors!" % self.json_input)

    def get_pipeline_name(self):
        # Creates a unique name for the pipeline
        # Concatenates the pipeline_id to either the name of a single sample or 'Multisample' in the case of >1 samples
        prefix = self.sample_data[self.sample_data.keys()[0]].name if len(self.sample_data) == 1 else "Multisample"
        return "%s-%s" % (prefix, self.pipeline_id)


