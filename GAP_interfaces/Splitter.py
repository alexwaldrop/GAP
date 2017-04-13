import logging
import abc

class Splitter(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.splits = None
        self.final_output = None

    def get_nr_splits(self):
        return len(self.get_splits())

    def get_splits(self):
        if self.splits is None:
            logging.error("No splits data is defined! Please initialize the attribute \"nr_splits\" with the splits data!")
            raise NotImplementedError("Splitter class does not have a required \"splits\" attribute!")

        return self.splits

    def get_final_output(self):
        return self.final_output

    @abc.abstractmethod
    def get_command(self, **kwargs):
        raise NotImplementedError("Splitter class does not have a required \"get_command()\" method!")
