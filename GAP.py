#!/usr/bin/env python2.7

import time
from datetime import datetime

from GAP_system import Config, Node
from GAP_modules import GoogleCompute as Platform
from GAP_modules import GoogleException

# Generating the config object
config = Config("Config/GAP.config", silent = True).config

if config["sample"]["ref"] == "hg19":
    config["paths"]["ref"] = "/ref/hg19/ucsc.hg19.fasta"

def main():
    global plat

    # Setting up the platform
    plat = Platform(config)
    plat.prepare_data(config["sample"], nr_local_ssd=5)

    # Running the alignment
    Node(config, plat, config["sample"], "BwaAligner").run()

    # Copy the final results to the bucket
    plat.finalize(config["sample"])

    # Aligning done
    print ("[%s] DONE!" % datetime.now())
    time.sleep(300)

try:
    main()
except (KeyboardInterrupt, GoogleException):
    print(" NOW EXITING!")
    del plat
