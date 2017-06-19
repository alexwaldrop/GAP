import json
from IO import PipelineFile

class Sample(dict):

    def __init__(self, kwargs):
        super(Sample, self).__init__(kwargs)

        # Initialize required keys
        self.name       = None
        self.input_data = None

        # Parse the current data
        self.parse_data()

    def __str__(self):
        data = json.dumps(self.input_data, indent=4)
        return "Sample: %s\n%s" % (self.name, data)

    def parse_data(self):

        # Get sample name
        self.name       = self.pop("name")

        # Create PipelineFile objects for sample files
        input_files     = self.pop("files")
        self.input_data = []
        for input_file in input_files:
            # Create PipelineFile with tags/metadata for any later use
            file_name       = input_file.pop("name")
            file_path       = input_file.pop("path")
            pipeline_file   = PipelineFile(file_name, file_path, is_main_input=True, **input_file)
            self.input_data.append(pipeline_file)

    def get_input_keys(self):
        return [input_file.get_file_type() for input_file in self.input_data]
