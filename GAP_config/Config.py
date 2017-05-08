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

        # Parse config
        self.parse_config()

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

    def parse_config(self):

        # Obtaining genomic reference name
        ref_name = self.config["sample"]["ref"]

        # Checking if reference is defined
        if ref_name not in self.config["references"]:
            logging.error("Reference %s definition was not found in the config file." % ref_name)
            exit(1)

        # Obtaining reference definition
        ref_dict = self.config["references"][ref_name]

        # Adding reference path to paths dictionary
        self.config["paths"]["ref"] = ref_dict["path"]

        # Processing the chromosome list
        chroms = list()
        for chrom_set in ref_dict["chroms"]:
            if "[" in chrom_set:
                head = chrom_set.split("[")[0]
                rang = chrom_set.split("[")[-1].split("]")[0]

                # Splitting ranges in subranges
                if "-" in rang:
                    limits = [int(val) for val in rang.split("-")]
                    chroms.extend( [head + str(i) for i in xrange(limits[0], limits[1] + 1)] )
                else:
                    chroms.append(head + rang)
            else:
                chroms.append(chrom_set)

        self.config["sample"]["chrom_list"] = chroms