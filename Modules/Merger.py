from Module import Module

class Merger(Module):

    def __init__(self, module_id):
        super(Merger, self).__init__(module_id)

    def define_input(self):
        raise NotImplementedError(
            "Merger module %s must implement 'define_input()' function!" % self.__class__.__name__)

    def define_output(self):
        raise NotImplementedError(
            "Merger module %s must implement 'define_output()' function!" % self.__class__.__name__)

    def define_command(self):
        raise NotImplementedError(
            "Merger module %s must implement 'define_command()' function!" % self.__class__.__name__)