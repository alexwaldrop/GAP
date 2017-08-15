import json
import logging

from jsonschema import Draft4Validator

from Config.Parsers import BaseParser

class JsonParser(BaseParser):
    # Class for parsing information from a JSON config file
    def __init__(self, config_file, config_spec_file):
        super(JsonParser, self).__init__(config_file, config_spec_file)

    def read_config(self):
        # Parse config data from JSON file and return JSON
        return self.parse_json(self.config_file)

    def validate_config(self):
        # Parse config spec file
        spec = self.parse_json(self.config_spec_file)

        # create validator object
        validator = Draft4Validator(spec)

        # validate config against schema and throw errors if invalid
        try:
            validator.validate(self.config)
        except:
            # raise invalid schema error and return messages
            logging.error("Invalid config: %s" % self.config_file)
            errors = validator.iter_errors(self.config)
            for error in sorted(errors):
                logging.error(error.message)
            raise

    @staticmethod
    def parse_json(json_file):
        # read JSON formatted instance template config
        with open(json_file, "r") as fh:
            json_data = fh.read()

        # try to load json from file, return exception otherwise
        try:
            return json.loads(json_data)

        except:
            logging.error("Config parsing error! Input File is not valid JSON: %s" % json_file)
            raise
