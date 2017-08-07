#!/usr/bin/env python2.7

import sys

from Pipeline import Pipeline
from PipelineWorker import PipelineWorker

def main():

    # Create a pipeline object
    pipeline = Pipeline(sys.argv)

    try:
        # Load the pipeline
        pipeline.load()

        # Create pipeline worker
        pipeline_worker = PipelineWorker(pipeline)

        # Run the pipeline
        pipeline_worker.run()

    finally:
        # Clean up the pipeline
        pipeline.clean_up()

if __name__ == "__main__":
    main()