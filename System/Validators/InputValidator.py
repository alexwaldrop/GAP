import logging

from Validator2 import Validator
from System.Workers import Thread
from System.Datastore import GAPFile

class InputValidator(Validator):

    def __init__(self, resource_kit, sample_data, storage_helper, docker_helper):
        super(InputValidator, self).__init__()
        # Check whether all input files declared in resource kit and sample data exist
        self.resources  = resource_kit
        self.samples    = sample_data
        self.storage_helper = storage_helper
        self.docker_helper  = docker_helper
        self.inputs = []
        self.input_workers = []

    def validate(self):

        # Check resource kit paths
        self.inputs.extend(self.__get_resource_paths())

        # Check resource kit docker images
        self.inputs.extend(self.__get_docker_images())

        # Check sample data paths
        self.inputs.extend(self.__get_sample_data_paths())

        for input_file in self.inputs:
            if isinstance(input_file, GAPFile):
                logging.debug("Validating file '%s' with path: %s" % (input_file.get_file_id(), input_file))
                worker = InputFileWorker(input_file, self.storage_helper)

            else:
                logging.debug("Validating docker image: %s" % input_file.get_image_name())
                worker = DockerWorker(input_file, self.docker_helper)

            # Start validating input file
            worker.start()

        # Finalize all file worker threads
        for file_worker in self.input_workers:
            file_worker.finalize()

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print the available reports
        self.print_reports()

        return has_errors

    def __get_resource_paths(self):
        # Check whether all paths in resource kit exist
        # Obtain the resource paths
        paths = []
        res = self.resources.get_resources()
        # Check if each resource path is present
        for res_type in res:
            for res_name in res[res_type]:
                res_obj = res[res_type][res_name]
                paths.append(res_obj)
        return paths

    def __get_docker_images(self):
        # Return list of docker images in resource kit
        return self.resources.get_docker_images().values()

    def __get_sample_data_paths(self):
        # Return list of paths in sample data
        paths = []
        sample_paths = self.samples.get_paths()
        # Check if the path exists
        for input_type, sample_paths in sample_paths.iteritems():
            if isinstance(sample_paths, list):
                for path in sample_paths:
                    paths.append(path)
            else:
                paths.append(sample_paths)
        return paths


class MissingInputError(Exception):
    pass


class InputFileWorker(Thread):

    def __init__(self, input_file, storage_helper):
        self.input_file = input_file
        self.storage_helper = storage_helper
        super(InputFileWorker, self).__init__(err_msg="Unable to validate file '%s' with path: %s" % (input_file.get_file_id(), input_file))

    def task(self):
        # Check if path exists. If it does, get and set it's size (in GB)
        # Throw an error if path doesn't exist
        path_to_check = self.input_file.get_transferrable_path() if self.input_file.is_prefix() else self.input_file.get_path()
        if self.storage_helper.path_exists(path_to_check):
            file_size = self.storage_helper.get_file_size(self.input_file.get_transferrable_path())
            self.input_file.set_size(file_size)
        else:
            logging.error("Input file path %s does not exist!" % self.input_file)
            raise MissingInputError("Input file path does not exist!")


class DockerWorker(Thread):

    def __init__(self, docker_image, docker_helper):
        self.docker_image = docker_image
        self.docker_helper = docker_helper
        super(DockerWorker, self).__init__(err_msg="Unable to validate docker image: %s" % (docker_image.get_image_name()))

    def task(self):
        # Check if path exists. If it does, get and set it's size (in GB)
        # Throw an error if path doesn't exist
        image_name = self.docker_image.get_image_name()
        if self.docker_helper.image_exists(image_name):
            image_size = self.docker_helper.get_image_size(image_name)
            self.docker_image.set_size(image_size)
        else:
            logging.error("Docker image %s either does not exist or pull failed!" % image_name)
            raise MissingInputError("Input docker image does not exist!")

