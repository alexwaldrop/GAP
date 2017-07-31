import logging

from configobj import *
from validate import Validator

from BaseParser import BaseParser

class CfgParser(BaseParser):
    # Class for parsing information from a JSON config file
    def __init__(self, config_file, config_spec_file):
        super(CfgParser, self).__init__(config_file, config_spec_file)

    def __read_config(self):
        # Parse and return config data using ConfigObj
        try:
            return ConfigObj(self.config_file, configspec=self.config_spec_file)
        except:
            logging.error("Config parsing error! Config file is not valid INI: %s" % self.config_file)
            raise

    def __validate_config(self):
        # Validating schema
        validator = Validator()
        results = self.config.validate(validator, preserve_errors=True)

        # Reporting errors with file
        if results != True:
            error_string = "Invalid config error!\n"
            for (section_list, key, _) in flatten_errors(self.config, results):
                if key is not None:
                    error_string += '\tThe key "%s" in the section "%s" failed validation\n' % (
                    key, ', '.join(section_list))
                else:
                    logging.info('The following section was missing:%s \n' % (', '.join(section_list)))

            logging.error(error_string)

            self.valid = False
        else:
            self.valid = True

        if not self.valid:
            raise IOError("Config file did not pass validation against the spec file!")