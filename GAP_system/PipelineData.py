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
        self.sample_data    = self.make_samples()

        # Make sure all samples have same input_data keys
        self.validate_sample_keys()

        # Get name of sample on which pipeline is being run
        self.sample_name    = self.generate_sample_name()

        # Set unique name for pipeline
        self.pipeline_name  = self.generate_pipeline_name()

        # Dictionary for holding global output files produced by pipeline
        self.final_output   = dict()

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

    def make_samples(self):
        # Parses sample information and creates sample objects for each sample record in the JSON
        # Checks to make sure all samples contain the same input file keys
        sample_data = dict()
        for sample in self.data["samples"]:
            # Create sample object from next sample
            sample_name = sample["name"]

            # Check to make sure no duplicate samples exist
            if sample_name in sample_data:
                logging.error("Pipeline input JSON contains two or more samples with the name '%s'! "
                              "Sample names must be unique!" % sample_name)
                exit(1)

            sample_data[sample_name] = SampleData(sample_name, sample["input"])
        return sample_data

    def validate_sample_keys(self):
        # Check to make sure every sample has exactly the same input data keys
        input_keys = None
        for sample in self.sample_data:
            sample_keys = sorted(self.sample_data[sample].input_data.keys())
            if input_keys is None:
                input_keys = sample_keys
            elif "".join(sample_keys) != "".join(input_keys):
                logging.error("Samples do not all have identical input data keys on sample sheet! Make sure all input data keys are the same!")
                exit(1)

    def generate_sample_name(self):
            return self.sample_data[self.sample_data.keys()[0]].name if len(self.sample_data) == 1 else "Multisample"

    def generate_pipeline_name(self):
        # Creates a unique name for the pipeline
        # Concatenates the pipeline_id to either the name of a single sample or 'Multisample' in the case of >1 samples
            prefix = self.sample_data[self.sample_data.keys()[0]].name if len(self.sample_data) == 1 else "Multisample"
            return "%s-%s" % (prefix, self.pipeline_id)

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
        return self.sample_data[self.sample_data.keys()[0]].input_data.keys()

    def get_main_input_files(self):

        # Return dictionary of sample input data files
        input_files = dict()
        num_samples = len(self.sample_data.keys())

        for sample_name, sample in self.sample_data.iteritems():
            for file_type, file_name in sample.input_data.iteritems():
                if num_samples == 1:
                    # Case: One sample
                    input_files[file_type] = file_name
                elif file_type not in input_files:
                    # Case: Multisample
                    input_files[file_type] = [file_name]
                else:
                    input_files[file_type].append(file_name)
        return input_files

    def get_samples(self):
        return self.sample_data

    def get_sample_name(self):
        return self.sample_name

    def get_pipeline_name(self):
        return self.pipeline_name

    def get_final_output(self):
        return self.final_output




