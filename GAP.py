#!/usr/bin/env python2.7

import time
from GAP_system import Config, Node
from GAP_modules import GoogleCompute as Platform

# Generating the config object
config = Config("GAP.config", silent = True)

# Setting up some variables
config.general.nr_cpus = 1
config.general.split = True
config.general.nr_splits = 3

# Setting up a fake profile

# 5M reads R1/R2
s = { "R1_path":"gs://davelab_temp/R1.fastq.gz",
      "R2_path":"gs://davelab_temp/R2.fastq.gz"}

# 10M reads R1/R2
#s = { "R1_path":"gs://davelab_temp/R1_10M.fastq.gz",
#      "R2_path":"gs://davelab_temp/R2_10M.fastq.gz"}

# 50M reads R1/R2
#s = { "R1_path":"gs://davelab_temp/R1_50M.fastq.gz",
#      "R2_path":"gs://davelab_temp/R2_50M.fastq.gz"}

# 100M reads R1/R2
#s = { "R1_path":"gs://davelab_temp/R1_100M.fastq.gz",
#      "R2_path":"gs://davelab_temp/R2_100M.fastq.gz"}

# Whole genome
#s = { "R1_path":"gs://davelab_temp/HMNTTCCXX_s1_1_K19_0349_SL147226.fastq.gz",
#      "R2_path":"gs://davelab_temp/HMNTTCCXX_s1_1_K19_0349_SL147226.fastq.gz"}

# Setting up the platform
plat = Platform(config)
plat.prepareData(s)

# Running the alignment
Node(config, plat, s, "BwaAligner").run()

# Copy the final results to the bucket
plat.finalize(s)

# Aligning done
print ("DONE!")
time.sleep(300)
