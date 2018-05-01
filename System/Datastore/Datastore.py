class Datastore(object):
    # Object containing all available information to GAP modules at any given instance
    def __init__(self, resource_kit_file, sample_data_file):

        self.rk_file = resource_kit_file
        self.sd_file = sample_data_file

        self.resource_kit = None
        self.sample_data = None

