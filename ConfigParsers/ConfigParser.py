import json
import logging
import os

class ConfigParser(object):
    def __init__(self, config_file, config_spec_file):

        # Initializing the variables
        self.valid = False

        self.config_file = config_file
        self.config_spec_file = config_spec_file

        self.config = None

        # Check files exist
        self.check_files()

    def read_config(self):
        pass

    def validate_config(self):
        pass

    def check_files(self):
        # Checking if the config file exists
        if not os.path.isfile(self.config_file):
            logging.error("Config file not found: %s" % self.config_file)
            exit(1)

        # Checking if the config schema file exists
        if not os.path.isfile(self.config_spec_file):
            logging.error("Config specification file not found: %s" % self.config_spec_file)
            exit(1)

    def get_validated_config(self):
        if self.config is None:
            self.config = self.read_config()
            self.validate_config()
        return self.config