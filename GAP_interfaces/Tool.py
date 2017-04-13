import logging
import abc

class Tool(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.output     = None
        self.final_output = None

    def get_output(self):
        return self.output

    def get_final_output(self):
        return self.final_output

    @abc.abstractmethod
    def get_command(self, **kwargs):
        raise NotImplementedError("Class does not have a required \"get_command()\" method!")
