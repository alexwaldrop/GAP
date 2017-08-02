import os
import logging

from ConfigParser import ConfigParser
from Resource import Resource

class ResourceKit (object):
    # Container class that parses and holds resource objects declared in an external config file
    def __init__(self, resource_config_file):

        # Parse and validate ResourceKit config file
        resource_config_spec = "../resources/config_schemas/ResourceKit.validate"
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
            path          = resource_data.pop("path")
            resource_type = resource_data.pop("resource_type")
            resources[resource_name] = Resource(resource_name, path, resource_type, **resource_data)
        return resources

    def __organize_by_type(self):
        # Returns resource dictionary organized by resource type instead of resource name
        resources = {}
        for resource_name, resource in self.resources.iteritems():
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

    def update_path(self, src_path, dest_dir):
        # Searches resources for a path matching the src_path
        # Updates path to refelct transfer to dest_dir

        # Get name of file after transfer
        file_name = src_path.rstrip("/").split("/")[-1]
        new_path = os.path.join(dest_dir, file_name)

        # Search resources to find the correct path to update
        src_path_found = False
        for resource_type, resource_names in self.resources.iteritems():
            for resource_name, resource in resource_names.iteritems():
                if resource.get_path() == src_path:
                    resource.set_path(new_path)
                    src_path_found = True
                elif resource.get_containing_dir() == src_path:
                    resource.set_containing_dir(new_path)
                    src_path_found = True

        # Throw error if no path found of type 'path_type' with path 'src_path'
        if not src_path_found:
            logging.error("Unable to update resource path '%s'as no resource paths matched!" % src_path)
            raise IOError("Unable to update resource path!")

