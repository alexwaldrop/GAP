import os
import logging

from Config import ConfigParser
from GAPFile import GAPFile

class ResourceKit (object):
    # Container class that parses and holds resource objects declared in an external config file
    def __init__(self, resource_config_file):

        # Parse and validate ResourceKit config file
        resource_config_spec = "System/Datastore/ResourceKit.validate"
        config_parser        = ConfigParser(resource_config_file, resource_config_spec)
        self.config          = config_parser.get_config()

        # Resource name
        self.resources = self.__init_resource_files()
        self.resources = self.__organize_by_type()

    def __init_resource_files(self):
        # Parse resources listed in configs and convert to config objects
        # Return dictionary of resource objects indexed by resource name
        resources = {}
        for resource_id, resource_data in self.config.iteritems():
            path          = resource_data.pop("path")
            resource_type = resource_data.pop("resource_type")
            resources[resource_id] = GAPFile(resource_id, resource_type, path, **resource_data)
        return resources

    def __organize_by_type(self):
        # Returns resource dictionary organized by resource type instead of resource name
        resources = {}
        for resource_id, resource in self.resources.iteritems():
            resource_type = resource.get_type()
            if resource_type not in resources:
                resources[resource_type] = {}
                resources[resource_type][resource_id] = resource
            else:
                resources[resource_type][resource_id] = resource
        return resources

    def has_resource_type(self, resource_type):
        return resource_type in self.resources

    def get_resources(self, resource_type=None):
        if resource_type is None:
            return self.resources
        else:
            return self.resources[resource_type]
