#!/usr/bin/env python2.7

import sys
import os
import argparse
import logging

from System import GAPipeline

# Define the available platform modules
available_plat_modules = {
    "Google": "GooglePlatform",
    "Hardac": "SlurmPlatform",
}

def configure_argparser(argparser_obj):

    def platform_type(arg_string):
        value = arg_string.capitalize()
        if value not in available_plat_modules:
            err_msg = "%s is not a valid platform! " \
                      "Please view usage menu for a list of available platforms" % value
            raise argparse.ArgumentTypeError(err_msg)

        return available_plat_modules[value]

    def file_type(arg_string):
        """
        This function check both the existance of input file and the file size
        :param arg_string: file name as string
        :return: file name as string
        """
        if not os.path.exists(arg_string):
            err_msg = "%s does not exist!! " \
                      "Please provide a correct file!!" % arg_string
            raise argparse.ArgumentTypeError(err_msg)

        return arg_string

    # Path to sample set config file
    argparser_obj.add_argument("--input",
                               action="store",
                               #type=argparse.FileType('r'),
                               type=file_type,
                               dest="sample_set_config",
                               required=True,
                               help="Path to config file containing input files "
                                    "and information for one or more samples.")

    # Path to sample set config file
    argparser_obj.add_argument("--name",
                               action="store",
                               type=str,
                               dest="pipeline_name",
                               required=True,
                               help="Descriptive pipeline name. Will be appended to final output dir. Should be unique across runs.")

    # Path to pipeline graph config file
    argparser_obj.add_argument("--pipeline_config",
                               action='store',
                               #type=argparse.FileType('r'),
                               type=file_type,
                               dest='graph_config',
                               required=True,
                               help="Path to config file defining "
                                    "pipeline graph and tool-specific input.")

    # Path to resources config file
    argparser_obj.add_argument("--res_kit_config",
                               action='store',
                               #type=argparse.FileType('r'),
                               type=file_type,
                               dest='res_kit_config',
                               required=True,
                               help="Path to config file defining "
                                    "the resources used in the pipeline.")

    # Path to platform config file
    argparser_obj.add_argument("--plat_config",
                               action='store',
                               #type=argparse.FileType('r'),
                               type=file_type,
                               dest='platform_config',
                               required=True,
                               help="Path to config file defining "
                                    "platform where pipeline will execute.")

    # Name of the platform module
    available_plats = "\n".join(["%s (as module '%s')" % item for item in available_plat_modules.iteritems()])
    argparser_obj.add_argument("--plat_name",
                               action='store',
                               type=platform_type,
                               dest='platform_module',
                               required=True,
                               help="Platform to be used. Possible values are:\n%s" % available_plats,)

    # Verbosity level
    argparser_obj.add_argument("-v",
                               action='count',
                               dest='verbosity_level',
                               required=False,
                               default=0,
                               help="Increase verbosity of the program."
                                    "Multiple -v's increase the verbosity level:\n"
                                    "0 = Errors\n"
                                    "1 = Errors + Warnings\n"
                                    "2 = Errors + Warnings + Info\n"
                                    "3 = Errors + Warnings + Info + Debug")

    # Final output dir
    argparser_obj.add_argument("-o", "--output_dir",
                              action='store',
                              type=str,
                              dest="final_output_dir",
                              required=True,
                              help="Absolute path to the final output directory.")

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

def configure_import_paths():

    # Get the directory of the executable
    exec_dir = sys.path[0]

    # Add the modules paths to the python path
    sys.path.insert(1, os.path.join(exec_dir, "Modules/Tools/"))
    sys.path.insert(1, os.path.join(exec_dir, "Modules/Splitters/"))
    sys.path.insert(1, os.path.join(exec_dir, "Modules/Mergers/"))

    # Add the available platforms to the python path
    for plat in available_plat_modules:
        sys.path.insert(1, os.path.join(exec_dir, "System/Platform/%s" % plat))

def main():

    # Configure argparser
    argparser = argparse.ArgumentParser(prog="GAP")
    configure_argparser(argparser)

    # Parse the arguments
    args = argparser.parse_args()

    # Configure logging
    configure_logging(args.verbosity_level)

    # Configuring the importing locations
    configure_import_paths()

    # Create pipeline object
    pipeline = GAPipeline(pipeline_id=args.pipeline_name,
                          graph_config=args.graph_config,
                          resource_kit_config=args.res_kit_config,
                          sample_data_config=args.sample_set_config,
                          platform_config=args.platform_config,
                          platform_module=args.platform_module,
                          final_output_dir=args.final_output_dir)

    # Initialize variables
    err     = True
    err_msg = None

    try:
        # Load the pipeline components
        pipeline.load()

        # Validated pipeline inputs and configuration
        pipeline.validate()

        # Run the pipeline
        pipeline.run()

        # Indicate that pipeline completed successfully
        err = False

    except BaseException as e:
        logging.error("Pipeline failed!")
        logging.error("Pipeline failure error:\n%s" % e.message)
        err_msg = e.message
        pipeline.save_progress()
        raise

    finally:
        # Generate pipeline run report
        pipeline.publish_report(err=err, err_msg=err_msg)

        # Clean up the pipeline. Only remove temporary output if pipeline completed successfully.
        pipeline.clean_up()

if __name__ == "__main__":
    main()
