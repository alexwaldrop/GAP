#!/usr/bin/env python2.7

import time
from GAP_system import Config, Node
from GAP_modules import GoogleCompute as Platform

# Generating the config object
config = Config("GAP.config", silent = True)

# Setting up a fake profile
#s = { "R1_path":"gs://davelab_temp/R1_40M_TEST.fastq",
#      "R2_path":"gs://davelab_temp/R2_40M_TEST.fastq"}

s = { "R1_path":"gs://davelab_temp/R1_TEST.fastq.gz",
      "R2_path":"gs://davelab_temp/R2_TEST.fastq.gz"}

# Setting up the platform
plat = Platform(config)
plat.prepareData(s, nr_local_ssd=2)

# Running the alignment
Node(config, plat, s, "BwaAligner").run()

# Copy the final results to the bucket
plat.finalize(s)

# Aligning done
print ("DONE!")
time.sleep(300)
