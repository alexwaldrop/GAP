'''
Originally created by Alex Waldrop on Oct 20, 2016.
Modified by Razvan Panea.
'''

import os.path
import logging

from configobj import *
from validate import Validator

class Config(object):

    def __init__(self, config_file, config_spec_file="GAP_config/GAP_validate.config"):

        # Initializing the variables
        self.config = None
        self.valid = False
        self.config_file = config_file
        self.config_spec_file = config_spec_file

        # Reading config file
        self.read_config()

        # Validating the config file
        self.validate_config()

        # Standardize formatting of directory strings in config
        self.format_dirs()

        # Convert relative cloud storage paths to absolute paths
        self.make_cloud_paths_absolute()

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

    def format_dirs(self):
        # Standardize formatting of directories specified in config
        for path in self.config["paths"]:
            # Check to make sure path is a string and not a hash (i.e. the tool/resource sublists)
            if isinstance(self.config["paths"][path], basestring) and (path != "ref"):
                # Check to make sure the option hasn't been set to an empty string
                if self.config["paths"][path] is not None:
                    self.config["paths"][path] = self.format_dir(self.config["paths"][path])

    def format_dir(self, dir):
        # Takes a directory path as a parameter and returns standard-formatted directory string '/this/is/my/dir/'
        return dir.rstrip("/") + "/"

    def make_cloud_paths_absolute(self):
        # Converts relative paths to absolute paths
        # ASSUMES ALL RELATIVE PATHS ARE LOCATED ON CLOUD STORAGE

        # Make all cloud tool paths absolute
        for file_type, file_name in self.config["paths"]["tools"].iteritems():
            # Determine whether tool path is cloud or instance path
            if not file_name.startswith("/"):
                file_name = file_name.replace(self.config["paths"]["cloud_storage_tool_dir"],"")
                file_name = os.path.join(self.config["paths"]["cloud_storage_tool_dir"], file_name)
            self.config["paths"]["tools"][file_type] = file_name

        # Make all cloud resource paths absolute
        for file_type, file_name in self.config["paths"]["resources"].iteritems():
            # Determine whether tool path is cloud or instance path
            if not file_name.startswith("/"):
                file_name = file_name.replace(self.config["paths"]["cloud_storage_resource_dir"], "")
                file_name = os.path.join(self.config["paths"]["cloud_storage_resource_dir"], file_name)
            self.config["paths"]["resources"][file_type] = file_name
