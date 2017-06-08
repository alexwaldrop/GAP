from Config import Config
from GAP_IO import ResourceFile, PipelineFile

class GAPConfig(Config):
    # Special config extending base class to perform GAP-related functions
    def __init__(self, config_file, **kwargs):
        config_spec_file = kwargs.get("config_spec_file", "GAP_config/GAPConfig.validate")
        super(GAPConfig, self).__init__(config_file, config_spec_file)

        # Convert all paths to PipelineFile objects
        self.parse_path_info()

        # Validate path syntax
        self.validate_path_syntax()

    def parse_path_info(self):
        # Parses paths from config file and turns them into PipelineFile objects
        for path_type, path in self.config["paths"].iteritems():
            data = self.config["paths"][path_type]
            if path_type == "output_dir":
                # Create file object for output directory
                kwargs = data
                self.config["paths"][path_type] = PipelineFile(**kwargs)

            elif path_type != "resources":
                # Create file object for wrk, log, tmp, bin, resource_dir
                self.config["paths"][path_type] = PipelineFile(path=path, is_dir=True)

            elif path_type == "resources":
                # Create file object for each resource path
                for resource in data:
                    kwargs = data[resource]
                    self.config["paths"][path_type][resource] = ResourceFile(**kwargs)

    def validate_path_syntax(self):
        # Validates all PipelineFile objects
        for path_type, path in self.config["paths"].iteritems():
            if path_type != "resources":
                path.validate()
            elif path_type == "resources":
                for resource in self.config["paths"][path_type]:
                    self.config["paths"][path_type][resource].validate()