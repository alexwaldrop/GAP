import logging
import abc

class Tool(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.nr_cpus    = None
        self.mem        = None

        self.output     = None
        self.final_output = None

    def get_nr_cpus(self):
        if self.nr_cpus is None:
            logging.error(
                "No vCPUs count is defined! Please initialize the attribute \"nr_cpus\" with the number of vCPUs needed!")
            raise NotImplementedError("Class does not have a required \"nr_cpus\" attribute!")

        return self.nr_cpus

    def get_mem(self):
        if self.mem is None:
            logging.error(
                "No memory value is defined! Please initialize the attribute \"mem\" with the amount (in GB) of memory RAM needed!")
            raise NotImplementedError("Class does not have a required \"mem\" attribute!")

        return self.mem

    def get_output(self):
        return self.output

    def get_final_output(self):
        return self.final_output

    @abc.abstractmethod
    def get_command(self, **kwargs):
        raise NotImplementedError("Class does not have a required \"get_command()\" method!")
