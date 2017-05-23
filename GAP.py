#!/usr/bin/env python2.7

import logging
import sys

from GAP_config import Config
from GAP_system import NodeManager
from GAP_modules.Google import GoogleCompute as Platform
from GAP_modules.Google import GoogleException
from GAP_system import PipelineData
from GAP_system import SampleData

# Initilizing global variables
config = None
plat = None

def configure_logging(config):

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
        logging.addLevelName(logging.ERROR,     "%s%s%s%s%s" % (BOLD, ERROR,    "GAP_ERROR",    END, END))
        logging.addLevelName(logging.WARNING,   "%s%s%s%s%s" % (BOLD, WARNING,  "GAP_WARNING",  END, END))
        logging.addLevelName(logging.INFO,      "%s%s%s%s%s" % (BOLD, INFO,     "GAP_INFO",     END, END))
        logging.addLevelName(logging.DEBUG,     "%s%s%s%s%s" % (BOLD, DEBUG,    "GAP_DEBUG",    END, END))
    else:
        logging.addLevelName(logging.ERROR,     "GAP_ERROR")
        logging.addLevelName(logging.WARNING,   "GAP_WARNING")
        logging.addLevelName(logging.INFO,      "GAP_INFO")
        logging.addLevelName(logging.DEBUG,     "GAP_DEBUG")

    # Setting the level of the logs
    level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][ config["general"]["verbosity"] ]
    logging.getLogger().setLevel(level)

def main():
    global plat
    global config

    # Obtain the config object
    config = Config("GAP_config/GAP.config").config

    # Configure the logging system
    configure_logging(config)

    data = PipelineData("/home/alex/Desktop/cloud_pipeline_development/test_input.json")

    exit(0)

    # Create platform
    plat = Platform(config)

    # Create Node Manager
    node_manager = NodeManager(config, plat)

    # Check I/O of the pipeline, before starting the pipeline
    node_manager.check_nodes()

    # Setting up the platform
    plat.prepare_platform(config["sample"])

    # Update NodeManager with changes made to config while preparing platform
    # paths of tools/resources may have changed if they were transferred from bucket to instance
    node_manager.update()

    # Running the modules
    node_manager.run()

    # Copy the final results to the bucket
    plat.finalize(config["sample"])

    # Aligning done
    logging.info("Analysis pipeline complete.")

if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        logging.info("Ctrl+C received! Now exiting!")
        raise
    except GoogleException:
        logging.info("Now exiting!")
        plat.finalize(config["sample"])
        raise
    finally:
        if plat is not None:
            plat.clean_up()
