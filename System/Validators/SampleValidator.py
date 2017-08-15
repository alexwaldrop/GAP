
from System.Validators import Validator

class SampleValidator(Validator):

    def __init__(self, pipeline_obj):
        super(SampleValidator, self).__init__(pipeline_obj)

    def __check_paired_end(self):

        sample_data = self.samples.get_data()

        if isinstance(sample_data["sample_name"], list):
            for sample_name, is_paired, R1_path, R2_path in zip(sample_data["sample_name"],
                                                                sample_data["is_paired"],
                                                                sample_data["R1"],
                                                                sample_data["R2"]):
                if is_paired:
                    if R2_path is None:
                        self.report_error("In sample data, sample '%s' is specified as paired end, "
                                          "but no R2 is provided." % (sample_name))

                else:
                    if R2_path is not None:
                        self.report_warning("In sample data, sample '%s' is specified as not paired end, but"
                                            "an R2 path is provided." % (sample_name))

        else:

            if sample_data["is_paired"]:
                if sample_data["R2"] is None:
                    self.report_error("In sample data, sample '%s' is specified as paired end, "
                                      "but no R2 is provided." % (sample_data["sample_name"]))

            else:
                if sample_data["R2"] is not None:
                    self.report_warning("In sample data, sample '%s' is specified as not paired end, but"
                                        "an R2 path is provided." % (sample_data["sample_name"]))

    def __check_paths_existence(self):

        # Obtain the paths from the sample data
        paths = self.samples.get_paths()

        # Check if the path exists
        for input_key, paths in paths.iteritems():
            if isinstance(paths, list):
                for path in paths:
                    if not self.platform.path_exists(path):
                        self.report_error("In sample data, the path for '%s' does not exist." % input_key)
            else:
                if not self.platform.path_exists(paths):
                    self.report_error("In sample data, the path for '%s' does not exist." % input_key)

    def validate(self):

        # Check if the sample is paired end and if all data is available
        self.__check_paired_end()

        # Check if the sample paths exist
        self.__check_paths_existence()

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print reports
        self.print_reports()

        return has_errors