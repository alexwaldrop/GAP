import os

from System.Validators import GraphValidator
from System.Validators import InputValidator
from System.Validators import SampleValidator
from System.Platform import StorageHelper, DockerHelper
from System.Datastore import Datastore
from System.Graph import Scheduler


class GAPipeline(object):

    def __init__(self, pipeline_id, graph, resource_kit, sample_data, platform):

        # Components needed to run GAP
        self.pipeline_id    = pipeline_id
        self.graph          = graph
        self.resource_kit   = resource_kit
        self.sample_data    = sample_data
        self.platform       = platform

        # Create datastore from pipeline components
        self.datastore      = Datastore(self.graph, self.resource_kit, self.sample_data, self.platform)

        # Task scheduler for running jobs
        self.scheduler = Scheduler(self.graph, self.datastore, self.platform)

        # Helper processor for handling platform operations
        self.helper_processor   = None
        self.storage_helper     = None
        self.docker_helper      = None

    def validate(self):

        # Assume all validations are working
        has_errors = False

        # Validate the sample set
        sample_validator = SampleValidator(self.sample_data)
        has_errors = sample_validator.validate() or has_errors

        # Validate the graph
        graph_validator = GraphValidator(self, self.resource_kit, self.sample_data)
        has_errors = graph_validator.validate() or has_errors

        # Validate the platform
        self.platform.validate()

        # Stop the pipeline before launching if there are any errors
        if has_errors:
            raise SystemError("One or more errors have been encountered during validation. "
                              "See the above logs for more information")

        # Create helper processor and storage/docker helpers for checking input files
        self.helper_processor   = self.platform.get_helper_processor()
        self.storage_helper     = StorageHelper(self.helper_processor)
        self.docker_helper      = DockerHelper(self.helper_processor)

        # Validate all pipeline inputs can be found on platform
        input_validator = InputValidator(self.resource_kit, self.sample_data, self.storage_helper, self.docker_helper)
        has_errors = input_validator.validate() or has_errors

        # Stop the pipeline if there are any errors
        if has_errors:
            raise SystemError("One or more errors have been encountered during validation. "
                              "See the above logs for more information")

        # Validate that workspace can be created
        workspace = self.datastore.get_task_workspace()
        for dir_type, dir_path in workspace.get_workspace():
            self.storage_helper.mkdir(dir_path=str(dir_path), job_name="mkdir_%s" % dir_type, wait=True)

    def run(self):
        # Run until all tasks are complete
        self.scheduler.run()

        # Delete temporary workspace
        workspace = self.datastore.get_task_workspace()
        self.storage_helper.rm(src_path=workspace.get_tmp_output_dir(), job_name="rm_tmp_output", wait=True)

    def save_progress(self):
        pass

    def publish_report(self):
        pass

    def clean_up(self):

        # Cleaning up the platform
        if self.__platform is not None:
            self.__platform.finalize()