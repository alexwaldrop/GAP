from ConfigParser import ConfigParser
from Resource import Resource

class ResourceKit (object):
    # Container class that parses and holds resource objects declared in an external config file
    def __init__(self, resource_config_file):

        # Parse and validate ResourceKit config file
        resource_config_spec = "resources/config_schemas/ResourceKit.validate"
        config_parser        = ConfigParser(resource_config_file, resource_config_spec)
        self.config          = config_parser.get_config()

        # Resource name
        self.resources = self.__create_resources()
        self.resources = self.__organize_by_type()

    def __create_resources(self):
        # Parse resources listed in configs and convert to config objects
        # Return dictionary of resource objects indexed by resource name
        resources = {}
        for resource_name, resource_data in self.config.iteritems():
            path          = resource_data["path"]
            resource_type = resource_data["resource_type"]
            resources[resource_name] = Resource(resource_name, path, resource_type, **resource_data)
        return resources

    def __organize_by_type(self):
        # Returns resource dictionary organized by resource type instead of resource name
        resources = {}
        for resource_name, resource in self.resources:
            resource_type = resource.get_type()
            if resource_type not in resources:
                resources[resource_type] = {}
                resources[resource_type][resource_name] = resource
            else:
                resources[resource_type][resource_name] = resource
        return resources

    def has_resource_type(self, resource_type):
        return resource_type in self.resources

    def get_resources(self, resource_type=None):
        if resource_type is None:
            return self.resources
        else:
            return self.resources[resource_type]
