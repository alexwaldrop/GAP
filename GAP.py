#!/usr/bin/env python2.7

import time
from GAP_system import Config
from GAP_modules import BwaAligner as Aligner
from GAP_modules import GoogleCompute as Platform

# Generating the config object
config = Config("GAP.config", silent = True)

# Setting up a fake profile
s = { "R1_path":"gs://davelab_temp/R1_40M_TEST.fastq",
      "R2_path":"gs://davelab_temp/R2_40M_TEST.fastq"}

# Setting up the platform
plat = Platform(config)
plat.prepareData(s, nr_local_ssd=2)

# Setting up the aligner
align = Aligner(config)
align.R1 = s["R1_new_path"]
align.R2 = s["R2_new_path"]
align.threads = 32

print (align.getCommand())

# Running the alignment command
plat.runCommand("align", align.getCommand(), on_instance = plat.main_server).wait()

# Aligning done
print ("DONE!")
time.sleep(300)
