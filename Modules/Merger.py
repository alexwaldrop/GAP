from Module import Module

class Merger(Module):

    def __init__(self, module_id, is_docker=False):
        super(Merger, self).__init__(module_id, is_docker)

    def define_input(self):
        raise NotImplementedError(
            "Merger module %s must implement 'define_input()' function!" % self.__class__.__name__)

    def define_output(self):
        raise NotImplementedError(
            "Merger module %s must implement 'define_output()' function!" % self.__class__.__name__)

    def define_command(self):
        raise NotImplementedError(
            "Merger module %s must implement 'define_command()' function!" % self.__class__.__name__)


class PseudoMerger(Module):
    # Module that can accept multiple inputs of the same type but cannot close split scope
    def __init__(self, module_id, is_docker=False):
        super(PseudoMerger, self).__init__(module_id, is_docker)

    def define_input(self):
        raise NotImplementedError(
            "PseudoMerger module %s must implement 'define_input()' function!" % self.__class__.__name__)

    def define_output(self):
        raise NotImplementedError(
            "PseudoMerger module %s must implement 'define_output()' function!" % self.__class__.__name__)

    def define_command(self):
        raise NotImplementedError(
            "PseudoMerger module %s must implement 'define_command()' function!" % self.__class__.__name__)