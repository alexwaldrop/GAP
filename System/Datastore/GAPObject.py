import logging

class GAPObjectMetadataError(Exception):
    # Base class for exception related to trying to access unavailable metadata field
    pass

class GAPObject (object):
    # Class to hold information related to a file
    def __init__(self, obj_type, name, val=None, **kwargs):

        # Object name
        self.name = name

        # Object type e.g. 'samtools', 'ref'
        self.type = obj_type

        # Object data
        self.value = val

        # Metadata associated with an object
        self.metadata = kwargs

    def get_name(self):
        return self.name

    def get_val(self):
        return self.value

    def get_type(self):
        return self.type

    def has_metadata_type(self, meta_type):
        return meta_type in self.metadata

    def get_metadata(self, meta_type):
        if not meta_type in self.metadata:
            logging.error("GAPObject of type '%s' named '%s' doesn't have metadata of type '%s'" % (self.type, self.name, meta_type))
            raise GAPObjectMetadataError("GAPObject does not have metadata of type '%s'" % meta_type)
        return self.metadata[meta_type]

    def set_metadata(self, meta_type, val):
        self.metadata[meta_type] = val

    def set_val(self, val):
        self.value = val
