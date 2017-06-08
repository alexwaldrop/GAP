import os.path
import logging

from configobj import *
from validate import Validator

class GAPConfig(dict):

    def __init__(self, config_file, config_spec_file):

        # Initializing the variables
        self.config = None
        self.valid  = False

        self.config_file = config_file
        self.config_spec_file = config_spec_file

        # Reading config file
        self.read_config()

        # Validating the config file
        self.validate_config()

        super(GAPConfig, self).__init__(self.config)


    def read_config(self):

        # Checking if the config file exists
        if not os.path.isfile(self.config_file):
            logging.error("Config file not found!")
            exit(1)

        if not os.path.isfile(self.config_spec_file):
            logging.error("Config specification file not found!")
            exit(1)

        # Attempting to parse the config file
        try:
            self.config = ConfigObj(self.config_file, configspec=self.config_spec_file)
        except:
            logging.error("Config parsing error! Invalid config file format.")
            raise

    def validate_config(self):

        # Validating schema
        validator = Validator()
        results = self.config.validate(validator, preserve_errors=True)

        # Reporting errors with file
        if results != True:
            error_string = "Invalid config error!\n"
            for (section_list, key, _) in flatten_errors(self.config, results):
                if key is not None:
                    error_string += '\tThe key "%s" in the section "%s" failed validation\n' % (key, ', '.join(section_list))
                else:
                    logging.info('The following section was missing:%s \n' % (', '.join(section_list)) )

            logging.error(error_string)

            self.valid = False
        else:
            self.valid = True

        if not self.valid:
            exit(1)
