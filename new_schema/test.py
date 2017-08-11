#!/usr/bin/env python

from ConfigParser import ConfigParser
from JsonParser import JsonParser
from SampleSet import SampleSet
from ResourceKit import ResourceKit

from GoogleStandardProcessor import GoogleStandardProcessor
from GooglePreemptibleProcessor import GooglePreemptibleProcessor
import time
import logging
import os
import subprocess as sp
import sys
from GooglePlatform import GooglePlatform

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

def authenticate(key_file):
    logging.info("Authenticating to the Google Cloud.")

    if not os.path.exists(key_file):
        logging.error("Authentication key was not found!")
        exit(1)
    cmd = "gcloud auth activate-service-account --key-file %s" % key_file
    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    if proc.wait() != 0:
        out, err = proc.communicate()
        logging.error("Authentication to Google Cloud failed!")
        logging.error("Recieved the following error:\n%s\n%s" % (out, err))
        exit(1)

    logging.info("Authentication to Google Cloud was successful.")

def main():
    sample_file = "/home/alex/Desktop/cloud_pipeline_development/test_runs/test_6_1_2017/test_sample_set.json"
    sample_data_spec     = "resources/config_schemas/SampleSet.validate"
    res_config = "resources/example_configs/ResourceKit.config"
    res_config_spec = "../resources/config_schemas/ResourceKit.validate"
    google_plat_config = "resources/example_configs/GooglePlatform.config"

    configure_logging(3)

    #
    # gp = GooglePlatform("derpity2", google_plat_config, "gs://testtesttest6969696969696/output_dir/newstuff/newerstuff/theabsoluteneweststuff/")
    # reskit = ResourceKit(res_config)
    # ss = SampleSet(sample_file)
    #
    # try:
    #
    #
    #     gp.launch_platform(reskit, ss)
    #
    #     r1 = ss.get_data("R1")
    #     r2 = ss.get_data("R2")
    #
    #     fastqc   = reskit.get_resources("fastqc")["fastqc"].get_path()
    #     qcparser = reskit.get_resources("qc_parser")["qc_parser"].get_path()
    #     java     = reskit.get_resources("java")["java"].get_path()
    #
    #     # Run fastqc
    #     fastqc_cmd = "%s -t %d --java %s --nogroup --extract %s %s !LOG3! ; sleep 300" % (fastqc, 3, java, r1, r2)
    #     gp.run_command("fastqc", fastqc_cmd, 3, 10)
    #     r1_out = os.path.join(gp.get_workspace_dir(),"%s_fastqc" % r1.replace(".fastq.gz", "").replace(".fastq", ""))
    #
    #     # Run QCParser
    #     fastqc_summary_file = os.path.join(r1_out, "fastqc_data.txt")
    #     output = "%s.fastqcsummary.txt" % fastqc_summary_file.split("_fastqc")[0]
    #     qc_parser_cmd = "%s fastqc -i %s -p DERP > %s !LOG2!" % (qcparser, fastqc_summary_file, output)
    #     gp.run_quick_command("qc_parser", qc_parser_cmd)
    #
    #     gp.return_output(r1_out)
    #     gp.return_output(output)
    #     gp.return_output(gp.get_workspace_dir("log"), log_transfer=False)
    #
    #     gp.main_processor.wait()
    #
    # except Exception:
    #     raise
    # finally:
    #     gp.clean_up()

main()
