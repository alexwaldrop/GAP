import logging
import os

from PipelineFile import PipelineFile

class ResourceFile(PipelineFile):
    # Extends pipeline file to hold information for pipeline resource files (e.g. tool binaries, ref genomes)
    def __init__(self, **kwargs):
        # Call super to init PipeilneFile
        super(ResourceFile, self).__init__(**kwargs)

        # Path to a containing directory that must transferred
        self.containing_dir   = kwargs.get("containing_dir", None)

        # Format containing dir path if necessary
        if self.containing_dir is not None:
            self.containing_dir = self.containing_dir.rstrip("/") + "/"

        # Make path absolute if it's relative to the containing directory
        self.make_path_absolute()
        print self.path

    def make_path_absolute(self):
        # Make absolute path if file path is relative to containing_dir
        # Assumes that paths that don't begin with "/" are relative to the containing dir
        if self.containing_dir is not None:
            if not self.path.startswith("/"):
                file_name = self.path.replace(self.containing_dir, "")
                self.path = os.path.join(self.containing_dir, file_name)

    def get_required_dir(self):
        return self.containing_dir

    def validate(self):
        # Validate that main path is contained within the required directory (if one was specified)
        if self.containing_dir is not None and not self.path.startswith(self.containing_dir):
            logging.error("Resource file (%s) is not found in its required directory (%s)! "
                          "Please provide the path to the actual directory containing the file in the config." %
                          (self.path, self.containing_dir))
            raise IOError("One or more resource files specified in the config was not found in its required directory. "
                          "See above for details!")






