#!/usr/bin/env python2.7

import time
from datetime import datetime

from GAP_system import Config, Node
from GAP_modules import GoogleCompute as Platform
from GAP_modules import GoogleException

# Generating the config object
config = Config("GAP.config", silent = True)

# Setting up some variables
config.general.nr_cpus = 8
config.general.mem = 25
config.general.split = True
config.general.nr_splits = 10

# Setting up a fake profile

# 5M reads R1/R2
s = { "R1_source":"gs://davelab_temp/R1.fastq.gz",
      "R2_source":"gs://davelab_temp/R2.fastq.gz",
      "sample_name":"Test_5M"}

# 10M reads R1/R2
#s = { "R1_source":"gs://davelab_temp/R1_10M.fastq.gz",
#      "R2_source":"gs://davelab_temp/R2_10M.fastq.gz",
#      "sample_name":"Test_10M"}

# 50M reads R1/R2
#s = { "R1_source":"gs://davelab_temp/R1_50M.fastq.gz",
#      "R2_source":"gs://davelab_temp/R2_50M.fastq.gz",
#      "sample_name":"Test_50M"}

# 100M reads R1/R2
#s = { "R1_source":"gs://davelab_temp/R1_100M.fastq.gz",
#      "R2_source":"gs://davelab_temp/R2_100M.fastq.gz",
#      "sample_name":"Test_100M"}

# Whole genome
#s = { "R1_source":"gs://davelab_fastq/HiSeq/HMNTTCCXX/HMNTTCCXX_s7_1_C23_0014_SL147232.fastq.gz",
#      "R2_source":"gs://davelab_fastq/HiSeq/HMNTTCCXX/HMNTTCCXX_s7_2_C23_0014_SL147232.fastq.gz",
#      "sample_name":"SL147232"}

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

