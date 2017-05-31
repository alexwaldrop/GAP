#!/usr/bin/env python2.7

import logging
from GAP_system.Pipeline import Pipeline

def main():

    sample_input = "/home/alex/Desktop/cloud_pipeline_development/test_input.json"
    config_file  = "GAP_config/GAP.config"

    pipeline = Pipeline(sample_input, config_file)
    pipeline.run()

    logging.info("Analysis pipeline complete.")

if __name__ == "__main__":
    main()