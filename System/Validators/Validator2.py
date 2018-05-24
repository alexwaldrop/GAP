import logging

class Validator(object):

    def __init__(self, datastore):

        # Initializing validator name
        self.name = self.__class__.__name__

        # Obtaining the pipeline components
        self.datastore  = datastore
        self.graph      = datastore.graph
        self.resources  = datastore.resource_kit
        self.samples    = datastore.sample_data
        self.platform   = datastore.platform

        # Define responses
        self.warnings   = []
        self.errors     = []

    def report_warning(self, message):
        self.warnings.append(message)

    def report_error(self, message):
        self.errors.append(message)

    def has_errors(self):
        return len(self.errors) != 0

    def print_reports(self):

        while len(self.errors):
            logging.error("%s: %s" % (self.name, self.errors.pop()))

        while len(self.warnings):
            logging.warn("%s: %s" % (self.name, self.warnings.pop()))
