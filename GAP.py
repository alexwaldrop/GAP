#!/usr/bin/env python2.7

import logging
import argparse
import sys
from Main import GAP

def get_arg_parser():
    # Returns command line argument parser for GAP
    parser = argparse.ArgumentParser()

    # Path to sample sheet containing sample level information
    parser.add_argument('--input',
                        action='store',
                        dest='input_data',
                        required=True,
                        help="Path to JSON formatted sample sheet containing input files and information for one or more samples.")

    # Path to config file defining pipeline runtime parameters
    parser.add_argument('--pipeline_config',
                        action='store',
                        dest='pipeline_config',
                        required=True,
                        help="Path to INI formatted configuration file defining pipeline structure and resources")

    # Path to config file defining platform runtime
    parser.add_argument('--platform_config',
                        action='store',
                        dest='platform_config',
                        required=True,
                        help="Path to INI formatted configuration file defining platform where pipeline will execute")

    # String name of platform module to be created for running pipeline
    parser.add_argument('--platform_type',
                        action='store',
                        dest='platform_type',
                        required=True,
                        help="String specifying platform type (e.g. Google, Hardac, AWS)")

    # Verbosity
    parser.add_argument('-v',
                        action='store',
                        dest='verbosity',
                        required=False,
                        default=1,
                        type=int,
                        choices=[0,1,2,3],
                        help="Global verbosity level for ouptputting runtime status messages. 0 = Errors\n1=Errors + Warnings\n2=Errors + Warnings + Info\n3=Errors + Warnings + Info + Debug")


    return parser

def configure_logging(verbosity):

    # Setting the format of the logs
    FORMAT = "[%(asctime)s] %(levelname)s: %(message)s"

    # Configuring the logging system to the lowest level
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, stream=sys.stderr)

    # Defining the ANSI Escape characters
    BOLD = '\033[1m'
    DEBUG = '\033[92m'
    INFO = '\033[94m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    END = '\033[0m'

    # Coloring the log levels
    if sys.stderr.isatty():
        logging.addLevelName(logging.ERROR, "%s%s%s%s%s" % (BOLD, ERROR, "GAP_ERROR", END, END))
        logging.addLevelName(logging.WARNING, "%s%s%s%s%s" % (BOLD, WARNING, "GAP_WARNING", END, END))
        logging.addLevelName(logging.INFO, "%s%s%s%s%s" % (BOLD, INFO, "GAP_INFO", END, END))
        logging.addLevelName(logging.DEBUG, "%s%s%s%s%s" % (BOLD, DEBUG, "GAP_DEBUG", END, END))
    else:
        logging.addLevelName(logging.ERROR, "GAP_ERROR")
        logging.addLevelName(logging.WARNING, "GAP_WARNING")
        logging.addLevelName(logging.INFO, "GAP_INFO")
        logging.addLevelName(logging.DEBUG, "GAP_DEBUG")

    # Setting the level of the logs
    level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][verbosity]
    logging.getLogger().setLevel(level)

def main():

    from Pipeline import PipelineInput
    from IO import PlatformFile
    from IO import PlatformFileSet
    import os

    input_data = PipelineInput("/home/alex/Desktop/cloud_pipeline_development/test_runs/test_6_1_2017/test_input.json")
    pip_files  = input_data.get_main_input()

    for pip_file in pip_files:
        print pip_file.debug_print()

    #pip_files = pip_files.filter_by_metadata(key="sample", value="Booty")
    derp_file = pip_files[0]
    derp_file.set_file_type("POOP")

    #pip_files = pip_files.filter_by_tag("input")


    for pip_file in pip_files:
        print pip_file.debug_print()


    # Get options from command line
    #parser  = get_arg_parser()
    #args    = parser.parse_args(sys.argv[1:])

    #input_data = args.input_data
    #pipeline_config = args.pipeline_config
    #platform_config = args.platform_config
    #platform_type   = args.platform_type
    #verbosity       = args.verbosity

    # Configure logging system
    #configure_logging(verbosity)

    # Create GAP pipeline runner object
    #gap = GAP(platform_type=platform_type,
    #          input_data_file=input_data,
    #          platform_config_file=platform_config,
    #          pipeline_config_file=pipeline_config)

    # Run pipeline
    #gap.run()
    #logging.info("Analysis pipeline complete.")

if __name__ == "__main__":
    main()