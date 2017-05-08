#!/usr/bin/env python2.7

import logging

from GAP_config import Config
from GAP_system import NodeManager
from GAP_modules.Google import GoogleCompute as Platform
from GAP_modules.Google import GoogleException

# Initilizing global variables
config = None
plat = None

def main():
    global plat
    global config

    # Setting the format of the logs
    FORMAT = "[%(asctime)s] GAP_%(levelname)s: %(message)s"

    # Configuring the logging system to the lowest level
    logging.basicConfig(level=logging.DEBUG, format=FORMAT)

    # Creating logging
    config = Config("GAP_config/GAP.config").config

    # Setting the level of the logs
    if config["general"]["verbosity"] == 0:
        logging.getLogger().setLevel(logging.ERROR)
    elif config["general"]["verbosity"] == 1:
        logging.getLogger().setLevel(logging.WARNING)
    elif config["general"]["verbosity"] == 2:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create platform
    plat = Platform(config)

    # Create Node Manager
    node_manager = NodeManager(config, plat)

    # Check I/O of the pipeline, before starting the pipeline
    plat.check_input(config["sample"])
    node_manager.check_nodes()

    # Setting up the platform
    plat.prepare_platform(config["sample"])
    plat.prepare_data(config["sample"], nr_local_ssd=5)

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
        plat.finalize(config["sample"], only_logs=True)
        raise
    finally:
        plat.clean_up()
