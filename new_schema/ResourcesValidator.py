
from Validator import Validator

class ResourcesValidator(Validator):

    def __init__(self, pipeline_obj):
        super(ResourcesValidator, self).__init__(pipeline_obj)

    def __check_remote_paths(self):

        # Obtain the resource paths
        res = self.resources.get_resources()

        # Check if each resource path is present
        for res_type in res:
            for res_name in res[res_type]:
                res_obj = res[res_type][res_name]

                # Check only remote paths
                if not res_obj.is_remote():
                    continue

                # Check if remote path exists
                if not self.platform.path_exists(res_obj.get_path()):
                    self.report_error("In resources, the path to the resource '%s' of type '%s' does not exist."
                                      "Check if the containing directory "
                                      "or the path name is correct." % (res_name, res_type))

    def validate(self):

        # Check remote paths only, the local remote paths will be checked by the PlatformValidator
        self.__check_remote_paths()

        # Identify if there are errors before printing them
        has_errors = self.has_errors()

        # Print the available reports
        self.print_reports()

        return has_errors