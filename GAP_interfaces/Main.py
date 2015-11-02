from datetime import datetime
import abc

class Main():  
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        self.verbosity  = config.general.verbosity
    
    def error(self, text, terminate=True):
        print("[%s] GAP_ERR: %s" % (str(datetime.now()), text))

        if terminate:
            exit(1)

    def warning(self, text):
        if self.verbosity >= 1:
            print("[%s] GAP_WARN: %s" % (str(datetime.now()), text))

    def message(self, text):
        if self.verbosity == 2:
            print("[%s] GAP_MSG: %s" % (str(datetime.now()), text))
 
    @abc.abstractmethod
    def validate(self):
        pass
