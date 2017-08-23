import os
import importlib

from System.Graph import Graph
from System.ResourceKit import ResourceKit
from System.SampleSet import SampleSet
from System.Validators import GraphValidator
from System.Validators import PlatformValidator
from System.Validators import ResourcesValidator
from System.Validators import SampleValidator


class Pipeline(object):

    def __init__(self, args):

        # Obtain the paths to the configuration file from the args
        self.__pipeline_config    = args.graph_config
        self.__res_kit_config     = args.res_kit_config
        self.__sample_set_config  = args.sample_set_config
        self.__platform_config    = args.platform_config

        # Obtain the platform module name
        self.__plat_module        = args.platform_module

        # Obtain the final output directory
        self.__final_output_dir   = args.final_output_dir

        # Obtain pipeline name and append to final output dir
        self.__name                 = args.pipeline_name
        self.__final_output_dir     = os.path.join(self.__final_output_dir, self.__name)

        # Initialize pipeline components
        self.__graph      = None
        self.__resources  = None
        self.__samples    = None
        self.__platform   = None

    def load(self):

        # Assume all validations are working
        has_errors = False

        # Load resources
        self.__resources = ResourceKit(self.__res_kit_config)

        # Load the sample data
        self.__samples = SampleSet(self.__sample_set_config)

        # Load the graph
        self.__graph = Graph(self.__pipeline_config)

        # Load platform
        plat_module = importlib.import_module(self.__plat_module)
        plat_class = plat_module.__dict__[self.__plat_module]
        self.__platform = plat_class(self.__name, self.__platform_config, self.__final_output_dir)

        # Validate the resource kit
        has_errors = ResourcesValidator(self).validate() or has_errors

        # Validate the sample set
        has_errors = SampleValidator(self).validate() or has_errors

        # Validate the graph
        has_errors = GraphValidator(self).validate() or has_errors

        # Stop the pipeline before launching if there are any errors
        if has_errors:
            raise SystemError("One or more errors have been encountered during validation. "
                              "See the above logs for more information")

        # Launch the platform
        self.__platform.launch_platform(self.__resources, self.__samples)

        # Validate the platform
        has_errors = PlatformValidator(self).validate() or has_errors

        # Stop the pipeline if there are any errors
        if has_errors:
            raise SystemError("One or more errors have been encountered during validation. "
                              "See the above logs for more information")

    def clean_up(self):

        # Cleaning up the platform
        if self.__platform is not None:
            self.__platform.finalize()

    def get_graph(self):
        return self.__graph

    def get_resource_kit(self):
        return self.__resources

    def get_sample_set(self):
        return self.__samples

    def get_platform(self):
        return self.__platform
