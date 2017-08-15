
from System.Validators import Validator

class PlatformValidator(Validator):

    def __init__(self, pipeline_obj):

        super(PlatformValidator, self).__init__(pipeline_obj)

    def __check_workspace_dir(self):

        # Obtain all the workspace directories
        dirs = {
            "wrk": "working",
            "bin": "binary",
            "tmp": "temporary",
            "lib": "library",
            "res": "resources",
        }

        for dir_type, dir_name in dirs.iteritems():
            dir_path = self.platform.get_workspace_dir(dir_type)
            if not self.platform.path_exists(dir_path):
                self.report_error("In platform, the workspace %s directory ('%s') is missing! "
                                  "Please create this directory "
                                  "in the init__workspace() method." % (dir_name, dir_path))

    def __check_resource_paths(self):

        for resource_type, resource_names in self.resources.get_resources().iteritems():
            for resource_name, resource_obj in resource_names.iteritems():

                # Obtain the resource object path
                path = resource_obj.get_path()

                # Add wildcard character is path is a prefix
                if resource_obj.is_prefix():
                    path += "*"

                # Check if the path exists
                if not self.platform.path_exists(path):
                    self.report_error("The resource '%s' of type '%s' has not been found on the platform. "
                                      "The resource path is %s. Please ensure the resource is defined correctly."
                                      % (resource_name, resource_type, path))

    def __check_input_data_paths(self):

        # Obtain sample data and paths
        sample_data  = self.samples.get_data()
        sample_paths = self.samples.get_paths()

        # Check the existance of each sample path
        for path_name, paths in sample_paths.iteritems():

            if isinstance(paths, list):
                for sample_name, path in zip(sample_data["sample_name"], paths):
                    if not self.platform.path_exists(path):
                        self.report_error("For sample '%s', the input data '%s' with path '%s' has not been "
                                          "found on the platform."
                                          "Please ensure the the input data is defined correctly."
                                          % (sample_name, path_name, path))

            else:
                if not self.platform.path_exists(paths):
                    self.report_error("For sample '%s', the input data '%s' with path '%s' has not been "
                                      "found on the platform."
                                      "Please ensure the the input data is defined correctly."
                                      % (sample_data["sample_name"], path_name, paths))

    def __check_final_output_dir(self):
        # Check that final output directory actually exists on platform
        if not self.platform.path_exists(self.platform.get_final_output_dir()):
            self.report_error("Platform final output directory '%s' does not exist! Please specify valid output directory!"
                              % self.platform.get_final_output_dir())

    def validate(self):

        # Check if final output directory exists
        self.__check_final_output_dir()

        # Check if workspace dir is completely initialized
        self.__check_workspace_dir()

        # Check if all the paths are loaded on the platform
        self.__check_resource_paths()
        self.__check_input_data_paths()

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print the available reports
        self.print_reports()

        return has_errors
