import logging

class PipelineFile(object):
    # Base class for holding file-related information
    def __init__(self, **kwargs):

        # Name of pipeline file e.g. samtools_0.1.9, gatk_1.7.8
        self.name           = kwargs.pop("name",        None)

        # Path to file or directory
        self.path           = kwargs.pop("path", "")

        # File type e.g. 'samtools' 'gatk'
        self.file_type      = kwargs.pop("file_type",   None)

        # Boolean flag for whether path is a file or a directory
        self.is_dir         = kwargs.pop("is_dir",      False)

        # Boolean flag for whether path refers to an actual file or is a basename for a set of files
        # E.g. BWA index files may be listed as /path/to/bwa_index and refer to a set of files sharing a common suffix
        self.is_basename = kwargs.pop("is_basename",    False)

        # Boolean flag for whether file exists on hard-disk where pipeline will be run
        self.is_remote_path   = kwargs.get("is_remote_path", False)

        # Standardize path if file refers to directory
        if self.is_dir:
            self.path = self.path.rstrip("/") + "/"

        # Set rest of attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    def overwrites(self, pipeline_file):
        # Boolean returns True if two pipeline files refer to same path. False otherwise
        return self.path == pipeline_file.path

    def is_file_type(self, file_type):
        # Returns whether PipelineFile is the same type as either another pipeline file or a string
        if isinstance(file_type, PipelineFile):
            # Case: file_type is PipelineFile
            return self.file_type == file_type.file_type
        else:
            # Case: file_type is string or other type
            return self.file_type == file_type

    def set_name(self, name):
        self.name = name

    def set_path(self, path):
        self.path = path

    def set_file_type(self, file_type):
        self.file_type = file_type

    def get_name(self):
        return self.name

    def get_path(self):
        return self.path

    def get_file_type(self):
        return self.file_type

    def is_basename(self):
        return self.is_basename

    def is_dir(self):
        return self.is_dir

    def is_remote_path(self):
        return self.is_remote_file

    def validate(self):
        # Base function to be overridden by inheriting classes
        pass

    def __str__(self):
        return str(self.path)

    def debug_print(self):
        atts = [a for a in dir(self) if not a.startswith("__") and not callable(getattr(self, a))]
        vals = [getattr(self, att) for att in atts]

        to_return = ""
        for i in range(len(atts)):
            to_return += "\t%s: %s" % (atts[i], vals[i])

        return to_return



