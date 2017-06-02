import json
import logging
import os

from SampleData import SampleData

class PipelineData(object):

    def __init__(self, json_input):

        # Obtain the JSON data
        self.json_input     = json_input
        self.json_data      = self.parse_json_input(required_keys=["pipeline_id","samples"])

        # Obtain sample data
        self.samples        = self.parse_samples()

        # Generate name variables
        self.pipeline_id    = self.json_data["pipeline_id"]
        self.sample_name    = self.samples.keys()[0] if len(self.samples) == 1 else "Multisample"
        self.pipeline_name  = "%s-%s" % (self.sample_name, self.pipeline_id)

        # Dictionary for holding global output files produced by pipeline
        self.final_output   = dict()

    def parse_json_input(self, required_keys=None):
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
                json_data = json.load(ft)

        except:
            logging.error("Pipeline data input file is not a valid JSON file: %s." % self.json_input)
            raise

        # Checks for the presence of specific keys in a JSON file
        if required_keys is not None:
            errors = False
            for key in required_keys:
                if key not in json_data:
                    errors = True
                    logging.error(
                        "Required key '%s' not found in Pipeline JSON input file: %s" % (key, self.json_input))

            if errors:
                raise IOError(
                    "One or more required keys was not found in the JSON input file: %s. See above for errors!" % self.json_input)

        return json_data

    def parse_samples(self):
        # Parses sample information and creates sample objects for each sample record in the JSON
        # Checks to make sure all samples contain the same input file keys

        # Generate sample data
        sample_data = dict()
        for sample in self.json_data["samples"]:

            # Create sample object from next sample
            sample_name = sample["name"]

            # Check to make sure no duplicate samples exist
            if sample_name in sample_data:
                logging.error("Pipeline input JSON contains two or more samples with the name '%s'! Sample names must be unique!" % sample_name)
                raise IOError("Two or more samples have the same name in the JSON input file")

            sample_data[sample_name] = SampleData(sample)

        # Check if every sample has exactly the same input data keys (possible human error)
        input_keys = None
        for sample in sample_data:

            sample_keys = sorted(sample_data[sample].input_data.keys())

            if input_keys is None:
                input_keys = sample_keys

            elif "".join(sample_keys) != "".join(input_keys):
                logging.warning("Samples do not all have identical input data keys on sample sheet!")
                break

        return sample_data

    def add_final_output(self, tool_id, module_name, output_file_type, output_file):

        # Add output file to dictionary structure of files to return
        if module_name not in self.final_output:
            self.final_output[module_name]          = dict()

        if tool_id not in self.final_output[module_name]:
            self.final_output[module_name][tool_id] = []

        # Append tuple of (output_file_type, output_file) to list of output files for the tool/module
        self.final_output[module_name][tool_id].append((output_file_type, output_file))

    def get_main_input_keys(self):
        # Return input data keys
        return self.samples.values()[0].get_input_keys()

    def get_main_input(self):

        return [sample.input_data for sample in self.samples.itervalues()]

    def get_samples(self):
        return self.samples

    def get_sample_name(self):
        return self.sample_name

    def get_pipeline_name(self):
        return self.pipeline_name

    def get_final_output(self):
        return self.final_output
