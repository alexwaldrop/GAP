import os
import logging
from Config import Config
import json
from jsonschema import Draft4Validator
from copy import deepcopy


class JSONConfig(Config):
    def __init__(self, config_file, config_spec_file):
        super(JSONConfig, self).__init__(config_file, config_spec_file)

    def read_config(self):
        return self.parse_json(self.config_file)

    def validate(self):
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

    def parse_json(self, json_file):
        # read JSON formatted instance template config
        template_file = open(json_file, "r")
        template_data = template_file.read()
        template_file.close()

        # try to load json from file, return exception otherwise
        try:
            return json.loads(template_data)

        except:
            logging.error("Config parsing error! Input File is not valid JSON: %s" % json_file)
            raise

