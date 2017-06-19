import logging
class PlatformFileSet(list):
    # Container class for a set of PipelineFile objects
    def __init__(self, files=None):
        # Accepts a list or dictionary of PipelineFile objects
        # Keeps track of internal list of objects but provides dictionary accession methods

        # List and Dictionary must hold exactly the same data
        if files is None:
            self.files = []
        elif isinstance(files, list):
            self.files = files
        elif isinstance(files, dict):
            self.files = self.listify_file_dict(files)
        else:
            logging.error("PlatformFileSet must be of type list or dict!")
            raise IOError("PlatformFileSet must be of type list or dict!")

        super(PlatformFileSet, self).__init__(self.files)

    def filter_by_attribute(self, attribute, value):
        files = [pip_file for pip_file in self.files if getattr(pip_file, attribute) == value]
        return PlatformFileSet(files)

    def filter_by_tag(self, tag):
        files = [pip_file for pip_file in self.files if pip_file.has_tag(tag)]
        return PlatformFileSet(files)

    def filter_by_metadata(self, key, value):
        files = [pip_file for pip_file in self.files if
                 (pip_file.has_metadata(key) and pip_file.get_metadata(key) == value)]
        return PlatformFileSet(files)

    def filter_by_type(self, type):
        files = [pip_file for pip_file in self.files if isinstance(pip_file, type)]
        return PlatformFileSet(files)

    def listify_file_dict(self, file_dict):
        files = []
        for key in file_dict:
            if not isinstance(file_dict[key], dict):
                if not isinstance(file_dict[key], list):
                    files.append(file_dict[key])
                else:
                    files.extend(file_dict[key])
            else:
                files.extend(self.listify_file_dict(file_dict[key]))
        return files


