from IO import PlatformFile
from ConfigParsers import INIConfigParser

class PipelineConfig(INIConfigParser):
    # Parses, validates, and holds config defining pipeline structure and required resources
    def __init__(self, config_file, config_spec_file="resources/config_schemas/Pipeline.validate"):
        super(PipelineConfig, self).__init__(config_file, config_spec_file)

        # Convert all paths to PlatformFile objects
        for file_name, file_info in self.config["paths"]:
            file_path = file_info.pop("path")
            # Create a Platform file object for the path
            self.config["paths"][file_name] = PlatformFile(file_name, file_path, **file_info)
