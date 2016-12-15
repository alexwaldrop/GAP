#!/usr/bin/env python2.7

import time
import logging
from datetime import datetime

from GAP_system import Config, Node
from GAP_modules import GoogleCompute as Platform
from GAP_modules import GoogleException

# Generating the config object
config = Config("Config/GAP.config", silent = True).config

if config["sample"]["ref"] == "hg19":
    config["paths"]["ref"] = "/ref/hg19/ucsc.hg19.fasta"

def setup_logging():

    # Setting the format of the logs
    FORMAT = "[%(asctime)s] GAP_%(levelname)s: %(message)s"

    # Setting the level of the logs
    if config["general"]["verbosity"] == 0:
        LEVEL = logging.ERROR
    elif config["general"]["verbosity"] == 1:
        LEVEL = logging.WARNING
    elif config["general"]["verbosity"] == 2:
        LEVEL = logging.INFO
    else:
        LEVEL = logging.DEBUG

    # Configuring the logging system
    logging.basicConfig(level=LEVEL, format=FORMAT)

def main():
    global plat

    # Setup logging
    setup_logging()

    # Setting up the platform
    plat = Platform(config)
    plat.prepare_data(config["sample"], nr_local_ssd=5)

    # Running the alignment
    Node(config, plat, config["sample"], "BwaAligner").run()

    # Copy the final results to the bucket
    plat.finalize(config["sample"])

    # Aligning done
    logging.info("Analysis pipeline complete.")
    time.sleep(300)

try:
    main()
except (KeyboardInterrupt, GoogleException):
    logging.info("Now exiting!")
    del plat
