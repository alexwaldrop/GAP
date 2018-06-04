
from Validator2 import Validator

class SampleValidator(Validator):

    def __init__(self, sample_data):
        super(SampleValidator, self).__init__()
        self.samples = sample_data

    def __check_paired_end(self):

        sample_data = self.samples.get_data()

        if "is_paired" not in sample_data:
            return

        if "R1" not in sample_data:
            return

        # Check all samples if >1 samples
        if isinstance(sample_data["sample_name"], list):

            sample_data["R2"] = sample_data["R2"] if "R2" in sample_data else [None]*len(sample_data["sample_name"])
            print sample_data

            for sample_name, is_paired, R1_path, R2_path in zip(sample_data["sample_name"],
                                                                [sample_data["is_paired"]],
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

        # Check only one samples
        else:
            # Create dummy variable for R2 if not present
            sample_data["R2"] = sample_data["R2"] if "R2" in sample_data else None

            if sample_data["is_paired"]:
                if sample_data["R2"] is None:
                    self.report_error("In sample data, sample '%s' is specified as paired end, "
                                      "but no R2 is provided." % (sample_data["sample_name"]))

            else:
                if sample_data["R2"] is not None:
                    self.report_warning("In sample data, sample '%s' is specified as not paired end, but"
                                        "an R2 path is provided." % (sample_data["sample_name"]))

    def validate(self):

        # Check if the sample is paired end and if all data is available
        self.__check_paired_end()

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print reports
        self.print_reports()

        return has_errors