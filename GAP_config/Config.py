import os.path
import logging

class Config(dict):
    # Base class for holding configuration data to be read from a file and validated against a spec/schema file
    # Can be extended to parse different kinds of config files (e.g. JSON, INI, YAML)
    def __init__(self, config_file, config_spec_file):

        # Initializing the variables
        self.valid  = False

        self.config_file = config_file
        self.config_spec_file = config_spec_file

        # Check files exist
        self.check_files()

        # Reading config file
        self.config = self.read_config()

        # Validating the config file
        self.validate_config()

        super(Config, self).__init__(self.config)

    def check_files(self):
        # Checking if the config file exists
        if not os.path.isfile(self.config_file):
            logging.error("Config file not found: %s" % self.config_file)
            exit(1)

        # Checking if the config schema file exists
        if not os.path.isfile(self.config_spec_file):
            logging.error("Config specification file not found: %s" % self.config_spec_file)
            exit(1)

    def read_config(self):
        return None

    def validate_config(self):
        pass


