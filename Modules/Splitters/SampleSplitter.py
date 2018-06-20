from Modules import Splitter

class SampleSplitter(Splitter):

    def __init__(self, module_id, is_docker=False):
        super(SampleSplitter, self).__init__(module_id, is_docker)

        self.output_keys = ["sample_name"]

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self):
        # Obtaining the arguments
        samples = self.get_argument("sample_name")
        for sample in samples:
            self.make_split(split_id=sample, visible_samples=[sample])

    def define_command(self):
        return None
