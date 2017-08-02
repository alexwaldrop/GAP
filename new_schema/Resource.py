import os

class Resource (object):
    # Class to hold information related to a file
    def __init__(self, name, path, resource_type, **kwargs):

        # Resource name
        self.name = name

        # Path to resource
        self.path = path

        # Resource type e.g. 'samtools' 'gatk'
        self.type = resource_type

        # Path to a containing directory where resource is found
        self.containing_dir = kwargs.pop("containing_dir", None)

        # Boolean flag for whether path refers to an actual file or is a basename for a set of files
        # E.g. BWA index files may be listed as /path/to/bwa_index and refer to a set of files sharing a common suffix
        self.__is_prefix = self.path.endswith("*")

        # Boolean flag for whether resource exists on remote storage disk
        self.__is_remote = ":" in self.path if self.containing_dir is None else ":" in self.containing_dir

        # Boolean for whether resource is an executable
        self.__is_executable = kwargs.pop("is_executable", False)

        # Boolean flag for whether resource is a library
        self.__is_library    = kwargs.pop("is_library", False)

        # Standardize aspects of the resource path provided
        self.__standardize()

    def __standardize(self):
        if self.containing_dir is not None:
            # Standardize file path of containing directory
            self.containing_dir = self.containing_dir.rstrip("/") + "/"

            # Make all path absolute from containing directory
            file_name = self.path.replace(self.containing_dir, "")
            self.path = os.path.join(self.containing_dir, file_name)

        if self.is_prefix():
            # Remove wildcard character from path is path is prefix
            self.path = self.path.replace("*","")

    def get_name(self):
        return self.name

    def get_path(self):
        return self.path

    def get_type(self):
        return self.type

    def get_containing_dir(self):
        return self.containing_dir

    def is_prefix(self):
        return self.__is_prefix

    def is_remote(self):
        return self.__is_remote

    def is_executable(self):
        return self.__is_executable

    def is_library(self):
        return self.__is_library

    def set_path(self, path):
        self.path = path

    def set_containing_dir(self, new_containing_dir):
        self.path           = self.path.replace(self.containing_dir, "")
        self.containing_dir = new_containing_dir
        self.__standardize()

    def __str__(self):
        to_return = "=============\n"
        to_return += "Name:\t%s\n" % self.name
        to_return += "Type:\t%s\n" % self.type
        to_return += "Path:\t%s\n" % self.path
        to_return += "Containing_dir:\t%s\n" % self.containing_dir
        to_return += "is_remote:\t%s\n" % self.__is_remote
        to_return += "is_executable:\t%s\n" % self.__is_executable
        to_return += "is_library:\t%s\n" % self.__is_library
        to_return += "is_prefix:\t%s\n" % self.__is_prefix
        to_return += "=============\n"
        return to_return
