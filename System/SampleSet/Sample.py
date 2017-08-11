class Sample(object):

    def __init__(self, sample_data):
        self.name  = sample_data.pop("name")
        self.paths = sample_data.pop("paths")
        self.data  = sample_data

    def get_name(self):
        return self.name

    def get_paths(self):
        return self.paths

    def get_data(self):
        return self.data
