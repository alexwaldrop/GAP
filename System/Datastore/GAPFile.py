import os
import GAPObject

class GAPFile (GAPObject):
    # GAPObject specifically related to files
    def __init__(self, obj_type, name, val, **kwargs):

        # Inherits from GAPObject
        super(GAPFile, self).__init__(obj_type, name, val)

        # Check to make sure value is string (Path)
        assert isinstance(self.value, basestring), "GAPFile value must be string! Recieved '%s' of type '%s'" % (val, type(val))

        # Path to a containing directory where resource is found
        self.containing_dir = kwargs.pop("containing_dir", None)

        # Boolean flag for whether path refers to an actual file or is a basename for a set of files
        # E.g. BWA index files may be listed as /path/to/bwa_index and refer to a set of files sharing a common suffix
        self.__is_prefix = self.value.endswith("*")

        # Boolean for whether resource is an executable
        self.__is_executable = kwargs.pop("export_to_path", False)

        # Boolean flag for whether resource is a library
        self.__is_library    = kwargs.pop("export_to_ld_lib", False)

        # File size
        self.size = kwargs.pop("file_size", None)

        # Standardize aspects of the resource path provided
        self.__standardize()

    def __standardize(self):
        if self.containing_dir is not None:
            # Standardize file path of containing directory
            self.containing_dir = self.containing_dir.rstrip("/") + "/"

            # Make all path absolute from containing directory
            file_name = self.value.replace(self.containing_dir, "")
            self.value = os.path.join(self.containing_dir, file_name)

        if self.is_prefix():
            # Remove wildcard character from path is path is prefix
            self.value = self.value.replace("*", "")

    def get_containing_dir(self):
        return self.containing_dir

    def get_size(self):
        return self.size

    def is_prefix(self):
        return self.__is_prefix

    def is_remote(self):
        return ":" in self.value if self.containing_dir is None else ":" in self.containing_dir

    def is_executable(self):
        return self.__is_executable

    def is_library(self):
        return self.__is_library

    def size_known(self):
        # Return true if size is known
        return self.size is not None

    def set_containing_dir(self, new_containing_dir):
        self.value           = self.value.replace(self.containing_dir, "")
        self.containing_dir = new_containing_dir
        self.__standardize()

    def __str__(self):
        to_return = "=============\n"
        to_return += "Name:\t%s\n" % self.name
        to_return += "Type:\t%s\n" % self.type
        to_return += "Path:\t%s\n" % self.value
        to_return += "Containing_dir:\t%s\n" % self.containing_dir
        to_return += "is_remote:\t%s\n" % self.is_remote()
        to_return += "is_executable:\t%s\n" % self.__is_executable
        to_return += "is_library:\t%s\n" % self.__is_library
        to_return += "is_prefix:\t%s\n" % self.__is_prefix
        to_return += "=============\n"
        return to_return
