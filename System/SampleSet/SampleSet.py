import os
import logging

from Config import ConfigParser
from Sample import Sample

class SampleSet (object):
    # Container class that parses, holds, and provides access to Sample-level data declared in an external config file
    def __init__(self, sample_data_json):

        # Parse and validate SampleSet config file
        sample_data_spec     = "Config/Schema/SampleSet.validate"
        config_parser        = ConfigParser(sample_data_json, sample_data_spec)
        self.config          = config_parser.get_config()

        # Create Sample Objects
        self.samples   = self.__create_samples()

        # Check that sample-level metadata types are identical for every sample
        self.__check_samples()

        # Organize Sample input paths by path type
        self.paths     = self.__organize_paths_by_type()

        # Organize global and sample-level metadata by data type
        self.data     = self.__organize_data_by_type()

    def __create_samples(self):
        # Parse sample data list in config and convert to Sample objects
        # Return dictionary of samples indexed by sample name
        samples = []
        for sample_data in self.config["samples"]:
            samples.append(Sample(sample_data))
        return samples

    def __check_samples(self):
        # Check that all samples contain the same sample-level metadata types
        first = True
        required_data = None
        for sample in self.samples:
            metadata_keys = "".join(sorted(sample.get_data().keys()))
            if first:
                required_data = metadata_keys
                first = False
            elif metadata_keys != required_data:
                # Check that all samples contain the same metadata
                    logging.error("Samples provided in SampleInputConfig have different sample-level metadata types! "
                                  "\nSamples must contain identical metadata types!")
                    raise IOError("One or more samples contains metadata not shared by all other samples!")

    def __organize_paths_by_type(self):
        # Combine paths from all samples and organize them by path type
        paths = {}
        for sample in self.samples:
            sample_paths = sample.get_paths()
            for path_type, path in sample_paths.iteritems():
                self.__add_data(paths, path_type, path)
        return paths

    def __organize_data_by_type(self):
        # Combine global and sample-level data into single dictionary organized by data type
        data = dict()

        # Add sample-level data
        for sample in self.samples:
            # Add sample name to data
            sample_name = sample.get_name()
            self.__add_data(data, "sample_name", sample_name)

            # Add sample-level metadata
            for sample_data_type, sample_data_val in sample.get_data().iteritems():
                self.__add_data(data, sample_data_type, sample_data_val)

        # Add any data not associated with a sample as global metadata
        for global_data_type, global_data_val in self.config.iteritems():
            if global_data_type != "samples":
                self.__add_data(data, global_data_type, global_data_val)

        # Add sample paths
        for path_type, path_data in self.paths.iteritems():
            data[path_type] = path_data

        return data

    def __add_data(self, data, key, value):
        # Add value of a data type to a dictionary
        if key in data:
            # If data type already appears in
            if(isinstance(data[key], list)):
                # Add to list that already exists
                data[key].append(value)
            else:
                # Create list from singleton value and add new value to list
                data[key] = [data[key], value]
        else:
            data[key] = value
        return data

    def has_data_type(self, data_type):
        return data_type in self.data

    def get_paths(self, path_type=None):
        if path_type is None:
            return self.paths
        else:
            return self.paths[path_type]

    def get_data(self, data_type=None):
        if data_type is None:
            return self.data
        else:
            return self.data[data_type]

    def update_path(self, src_path, dest_dir):
        # Searches in self.data for a path matching the src_path
        # Updates path to refelct transfer to dest_dir

        # Get name of file after transfer
        file_name   = src_path.split("/")[-1]
        new_path    = os.path.join(dest_dir, file_name)

        # Search through paths to find the correct path to update
        path_types      = self.paths.keys()
        src_path_found  = False
        for data_type, data in self.data.iteritems():
            # Check to see if next type is a path
            if data_type in path_types:
                # Iterate through all path if mutliple paths of this type
                if isinstance(data, list):
                    for i in range(len(data)):
                        if data[i] == src_path:
                            self.data[data_type][i] = new_path
                            src_path_found          = True
                # Check to see if path is src_path if only one path of this type
                elif data == src_path:
                    self.data[data_type]    = new_path
                    src_path_found          = True

        # Throw error if no path found of type 'path_type' with path 'src_path'
        if not src_path_found:
            logging.error("Unable to update sample path '%s'as no sample paths matched!" % src_path)
            raise IOError("Unable to update sample path!")
