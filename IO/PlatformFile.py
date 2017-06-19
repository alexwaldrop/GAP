import logging
import os

class PlatformFile(object):
    # Class to hold information related to a file
    def __init__(self, name, path, **kwargs):

        # Name of pipeline file e.g. samtools_0.1.9, gatk_1.7.8
        self.name = name

        # Path to file or directory
        self.path = path

        # File type e.g. 'samtools' 'gatk'
        self.file_type = kwargs.pop("file_type", None)

        # Boolean flag for whether path is a file or a directory
        self.is_dir = kwargs.pop("is_dir", False)

        # Boolean flag for whether path refers to an actual file or is a basename for a set of files
        # E.g. BWA index files may be listed as /path/to/bwa_index and refer to a set of files sharing a common suffix
        self.is_basename = kwargs.pop("is_basename", False)

        # Boolean flag for whether file exists on hard-disk where pipeline will be run
        self.is_remote_path = kwargs.pop("is_remote_path", False)

        # Path to a containing directory where file is found
        self.containing_dir = kwargs.pop("containing_dir", None)

        # Boolean for whether file is an executable/program
        self.is_executable = kwargs.pop("is_executable", False)

        # Boolean flag for whether file should be returned to output_dir on platform shutdown
        self.is_output = kwargs.pop("is_output", False)

        # Set of tags associated with the file
        self.tags = kwargs.pop("tags", [])

        # Save any remaining key-value pairs as additional metadata
        self.metadata = kwargs

        # Standardize aspects of the path provided
        self.standardize()

        # Validate path
        self.validate()

    def standardize(self):
        # Standardize path if file refers to directory
        if self.is_dir:
            self.path = self.path.rstrip("/") + "/"

        if self.containing_dir is not None:
            # Standardize file path of containing directory
            self.containing_dir = self.containing_dir.rstrip("/") + "/"

            # Make aboslute path if file path is relative
            # Assumes paths that don't start with "/" and have a containing dir are relative
            if not self.path.startswith("/"):
                file_name = self.path.replace(self.containing_dir, "")
                self.path = os.path.join(self.containing_dir, file_name)

        # Make sure tags is a list
        if not isinstance(self.tags, list):
            self.tags = [self.tags]

    def overwrites(self, pipeline_file):
        # Boolean returns True if two pipeline files refer to same path. False otherwise
        return self.path == pipeline_file.path

    def validate(self):
        # Validate that main path is contained within the required directory (if one was specified)
        if self.containing_dir is not None and not self.path.startswith(self.containing_dir):
            logging.error("File (%s) is not found in its required directory (%s)! "
                          "Please provide the path to the actual directory containing the file in the config." %
                          (self.path, self.containing_dir))
            raise IOError("One or more files specified in the config was not found in its required directory. "
                          "See above for details!")

    def set_name(self, name):
        self.name = name

    def set_path(self, path):
        self.path = path

    def set_file_type(self, file_type):
        self.file_type = file_type

    def set_tags(self, tags):
        self.tags = tags

    def set_containing_dir(self, containing_dir):
        self.containing_dir = containing_dir

    def set_as_output(self):
        self.is_output = True

    def unset_as_output(self):
        self.is_output = False

    def add_tag(self, tag):
        self.tags.append(tag)

    def remove_tag(self, tag):
        self.tags.remove(tag)

    def set_metadata(self, metadata):
        self.metadata = metadata

    def add_metadata(self, key, value):
        self.metadata[key] = value

    def remove_metadata(self, remove_key):
        self.metadata = {key: value for key, value in self.metadata.items()
                         if key is not remove_key}

    def get_name(self):
        return self.name

    def get_path(self):
        return self.path

    def get_file_type(self):
        return self.file_type

    def get_containing_dir(self):
        return self.containing_dir

    def is_dir(self):
        return self.is_dir

    def is_basename(self):
        return self.is_basename

    def is_remote_path(self):
        return self.is_remote_path

    def is_executable(self):
        return self.is_executable

    def is_output(self):
        return self.is_output

    def has_tag(self, tag_name):
        return tag_name in self.tags

    def has_metadata(self, key):
        return key in self.metadata

    def get_metadata(self, key):
        return self.metadata[key]

    def __str__(self):
        return str(self.path)

    def debug_print(self):
        atts = [a for a in dir(self) if not a.startswith("__") and not callable(getattr(self, a))]
        vals = [getattr(self, att) for att in atts]

        to_return = ""
        for i in range(len(atts)):
            to_return += "\t%s: %s" % (atts[i], vals[i])

        return to_return



