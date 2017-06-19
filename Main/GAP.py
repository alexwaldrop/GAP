import logging
import importlib

from Pipeline.PipelineInput import PipelineInput
from Pipeline.NodeManager import TaskGraph

class GAP(object):
    # Main class for running a GAP pipeline from set of input files
    def __init__(self, platform_type, input_data_file, platform_config_file, pipeline_config_file):

        # Parse and validate input data
        self.input_data = PipelineInput(input_data_file)
        self.name       = self.input_data.get_pipeline_name()

        # Import correct type of platform module
        self.platform_type = platform_type
        try:
            self.platform_module    = GAP.init_platform(self.platform_type)
        except ImportError:
            logging.error("%s Platform cannot be imported!" % self.platform_type)
            exit(1)

        # Initialize platform from config file
        self.platform = self.platform_module["class"](self.name, platform_config_file)

        # Initialize pipeline task graph from config file
        self.task_graph = TaskGraph(pipeline_config_file, self.input_data)

        self.success    = True

    def run(self, **kwargs):
        try:
            self.run_pipeline(**kwargs)
        except KeyboardInterrupt:
            self.success = False
            logging.info("Ctrl+C received! Now exiting!")
            raise
        except:
            logging.info("Now exiting!")
            self.success = False
            self.finalize()
            raise
        finally:
            self.clean_up()

    def run_pipeline(self, **kwargs):

        # Platform on which pipeline will be run
        # Build and check module dependency graph specified in config
        self.task_graph.validate()

        # Validate and launch platform where pipeline will be executed
        self.platform.launch_platform(**kwargs)

        # Run the modules
        self.task_graph.run()

        # Copy the final results to the bucket
        self.platform.finalize()

    def clean_up(self):
        if hasattr(self, "platform") and self.platform is not None:
            self.platform.clean_up()

    def finalize(self):
        if hasattr(self, "platform") and self.platform is not None:
            self.platform.finalize()

    @staticmethod
    def init_platform(platform_type):
        d = dict()
        d["platform_type"] = platform_type
        d["platform"] = importlib.import_module("Platform.%s.Platform" % d["platform_type"])
        d["class_name"] = d["platform"].__main_class__
        d["class"] = d["platform"].__dict__[d["class_name"]]
        return d

