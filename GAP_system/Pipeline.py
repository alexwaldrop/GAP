import logging
import sys

from GAP_system import PipelineData
from GAP_config import Config
from GAP_modules.Google import GooglePlatform
from GAP_system import NodeManager

class Pipeline(object):
    def __init__(self, sample_sheet_file, config_file):

        # Parse and validate config file and store as a dictionary
        self.config         = Config(config_file).config

        # Configure pipeline logging
        self.configure_logging()

        # Parse and validate sample sheet and store as a PipelineData object
        self.pipeline_data  = PipelineData(sample_sheet_file)

        self.platform       = None
        self.node_manager   = None

    def run(self, **kwargs):
        try:
            self.run_pipeline(**kwargs)
        except KeyboardInterrupt:
            logging.info("Ctrl+C received! Now exiting!")
            raise
        except:
            logging.info("Now exiting!")
            self.finalize()
            raise
        finally:
            self.clean_up()

    def run_pipeline(self, **kwargs):

        # Platform on which pipeline will be run
        self.platform = GooglePlatform(self.config, self.pipeline_data)

        # Create pipeline node manager and build pipeline dependency graph
        self.node_manager = NodeManager(self.platform)

        # Build and check module dependency graph specified in config
        self.node_manager.check_nodes()

        # Validate and launch platform where pipeline will be executed
        self.platform.launch_platform(**kwargs)

        # Run the modules
        self.node_manager.run()

        # Copy the final results to the bucket
        self.platform.finalize()

    def clean_up(self):
        if self.platform is not None:
            self.platform.clean_up()

    def finalize(self):
        if self.platform is not None:
            self.platform.finalize()

    def configure_logging(self):

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
        level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][self.config["general"]["verbosity"]]
        logging.getLogger().setLevel(level)











