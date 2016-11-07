from datetime import datetime
import abc

class Main():  
    __metaclass__ = abc.ABCMeta

    def __init__(self, config=None, silent=False):
        if config is None:
            self.verbosity = 2
        else:
            self.verbosity  = config["general"]["verbosity"]

        if silent:
            self.terminate = False
        else:
            self.terminate = True
    
    def error(self, text, terminate=True):
        print("[%s] GAP_ERR: %s" % (str(datetime.now()), text))

        if terminate and self.terminate:
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
