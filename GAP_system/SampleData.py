import logging

class SampleData:
    def __init__(self, name, input_data):
        self.name       = name
        self.input_data = input_data

        # Check to make sure input_file_dict is dict
        if not isinstance(self.input_data, dict):
            msg = "Failed to initialize SampleData with sample name '%s'! Input data must be a dictionary, received: %s" \
                  % (self.name, self.input_data)
            logging.error(msg)
            raise TypeError(msg)

    def __str__(self):
        return "Sample: %s\n%s" % (self.name, self.input_data)


