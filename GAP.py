#!/usr/bin/env python2.7

import time
from datetime import datetime


from GAP_system import Config, Node
from GAP_modules import GoogleCompute as Platform
from GAP_modules import GoogleException

# Generating the config object
config = Config("GAP.config", silent = True)

# Setting up some variables
config.general.nr_cpus = 32
config.general.mem = 80
config.general.split = True
config.general.nr_splits = 45
config.general.sample_name = "SL147232"

# Setting up a fake profile

# 5M reads R1/R2
#s = { "R1_path":"gs://davelab_temp/R1.fastq.gz",
#      "R2_path":"gs://davelab_temp/R2.fastq.gz"}

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
s = { "R1_path":"gs://davelab_fastq/HiSeq/HMNTTCCXX/HMNTTCCXX_s7_1_C23_0014_SL147232.fastq.gz",
      "R2_path":"gs://davelab_fastq/HiSeq/HMNTTCCXX/HMNTTCCXX_s7_2_C23_0014_SL147232.fastq.gz"}

# HMNTTCCXX_s7_1_C23_0014_SL147232.fastq.gz

def main():
    global plat

    # Setting up the platform
    plat = Platform(config)
    plat.prepare_data(s, nr_local_ssd=5)

    # Running the alignment
    Node(config, plat, s, "BwaAligner").run()

    # Copy the final results to the bucket
    plat.finalize(s)

    # Aligning done
    print ("[%s] DONE!" % datetime.now())
    time.sleep(300)

try:
    main()
except (KeyboardInterrupt, GoogleException):
    print(" NOW EXITING!")
    del plat

