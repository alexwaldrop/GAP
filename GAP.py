#!/usr/bin/env python2.7

import logging
from GAP_system.Pipeline import Pipeline
import argparse
import sys

def get_arg_parser():
    # Returns command line argument parser for GAP
    parser = argparse.ArgumentParser()

    # Path to config file defining pipeline runtime parameters
    parser.add_argument('--config',
                        action='store',
                        dest='config_file',
                        required=True,
                        help="Path to INI formatted configuration file defining pipeline runtime parameters")

    # Path to sample sheet containing sample level information
    parser.add_argument('--input',
                        action='store',
                        dest='sample_input_file',
                        required=True,
                        help="Path to JSON formatted sample sheet containing input files and information for one or more samples.")
    return parser


def main():

    # Get options from command line
    parser  = get_arg_parser()
    args    = parser.parse_args(sys.argv[1:])

    sample_input = args.sample_input_file
    config_file  = args.config_file

    pipeline = Pipeline(sample_input, config_file)
    pipeline.run()

    logging.info("Analysis pipeline complete.")

if __name__ == "__main__":
    main()