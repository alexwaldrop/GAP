import os.path

import configobj
from validate import Validator

from GAP_interfaces import Main

class Config(Main):

    def __init__(self, config_file, silent = False):

        Main.__init__(self, silent=silent)

        # General config validator
        self.validate_file = "Config/GAP_validate.config"

        # Reading config file
        self.config = self.get_config(config_file)

        # Validate the config file
        self.valid  = self.validate()
        self.config["valid"] = self.valid

    def get_config(self, config_file):

        # Checking if the config file exists
        if not os.path.isfile(config_file):
            self.error("Config file not found!")

        # Checking if the validator file exists
        if not os.path.isfile(self.validate_file):
            self.error("Config validate file not found!")

        return configobj.ConfigObj(config_file, configspec=self.validate_file)

    def validate(self):

        validator = Validator()
        results = self.config.validate(validator)

        if results == True:
            return True

        errors = configobj.flatten_errors(self.config, results)

        for section_list, key, exception in errors:
            print(section_list, key, exception)
            for section in section_list:
                if key is None:
                    self.error("In config file, section '%s' is missing!" % section)
                elif exception == False:
                    self.error("In config file, key '%s' from section '%s' is missing!" % (key, section))
                else:
                    self.error("In config file, key '%s' from section '%s' is invalid!" % (key, section))
                    raise exception

        return False