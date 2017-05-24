import logging
import json

class SampleData:
    def __init__(self, name, input_data):
        self.name       = name
        self.input_data = input_data

        # Check that input_data is actually a dict
        self.check_input_data()

    def check_input_data(self):
        # Check to make sure input_file_dict is dict
        if not isinstance(self.input_data, dict):
            msg = "Failed to initialize SampleData with sample name '%s'! Input data must be a dictionary, received: %s" \
                  % (self.name, self.input_data)
            logging.error(msg)
            raise TypeError(msg)

    def __str__(self):
        data = json.dumps(self.input_data, indent=4)
        return "Sample: %s\n%s" % (self.name, data)


