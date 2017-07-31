import os
import logging
import abc

class BaseParser(object):
    __metaclass__ = abc.ABCMeta

    # Base class for parsing information from a config file and validating it against a specification file
    def __init__(self, config_file, config_spec_file):

        # Path to config file
        self.config_file = config_file

        # Path to config validation file
        self.config_spec_file = config_spec_file

        # Check to make sure config file and config spec file actually exist
        self.__check_files()

        # Parse and validate config file and store as dictionary object
        self.config = self.__get_valid_config()

    def __check_files(self):
        # Checking if the config file exists
        if not os.path.isfile(self.config_file):
            logging.error("Config file not found: %s" % self.config_file)
            raise IOError("Config file not found!")

        # Checking if the config schema file exists
        if not os.path.isfile(self.config_spec_file):
            logging.error("Config specification file not found: %s" % self.config_spec_file)
            raise IOError("Config spec. file not found!")

    @abc.abstractmethod
    def __read_config(self):
        pass

    @abc.abstractmethod
    def __validate_config(self):
        pass

    def __get_valid_config(self):
        self.config = self.__read_config()
        self.__validate_config()
        return self.config

    def get_config(self):
        return self.config




