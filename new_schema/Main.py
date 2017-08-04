#!/usr/bin/env python2.7

import sys

from Pipeline import Pipeline
from Engine import Engine

def main():

    # Create a pipeline object
    pipeline = Pipeline(sys.argv)

    try:
        # Load the pipeline
        pipeline.load()

        # Create an engine
        engine = Engine(pipeline)

        # Run the engine
        engine.run()

    finally:
        # Clean up the pipeline
        pipeline.clean_up()

if __name__ == "__main__":
    main()