import logging
import json

class SampleData(dict):

    def __init__(self, kwargs):
        super(SampleData,self).__init__(kwargs)

        # Initialize required keys
        self.name       = None
        self.input_data = None

        # Parse the current data
        self.parse_data()

    def __str__(self):
        data = json.dumps(self.input_data, indent=4)
        return "Sample: %s\n%s" % (self.name, data)

    def parse_data(self):

        # Check if the sample has a name
        if "name" not in self:
            logging.error("One or more samples do not have the required key \"name\"!")
            raise KeyError("One or more samples do not have the required key \"name\"!")

        # Obtain the sample name
        self.name       = self.get("name")

        # Check if sample has input data specified
        if "input" not in self:
            logging.error("Sample \"%s\" does not have the required key \"input\" with the input data!" % self.name)
            raise KeyError("Sample \"%s\" does not have the required key \"input\" with the input data!" % self.name)

        # Obtain the input data
        self.input_data = self.get("input")

        # Check if input data is a dictionary
        if not isinstance(self.input_data, dict):
            msg = "Failed to initialize SampleData with sample name '%s'! Input data must be a dictionary, received: %s" \
                  % (self.name, self.input_data)
            logging.error(msg)
            raise TypeError(msg)

    def get_input_keys(self):
        return self.input_data.keys()
