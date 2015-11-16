from GAP_interfaces import Main
import abc

class Sorter(Main):
    __metaclass__ = abc.ABCMeta

    input_types     = ["bam"]
    output_types    = ["bam"]

    def __init__(self, config):
        Main.__init__(self, config)

        self.input_type     = ""
        self.output_type    = ""

        self.from_stdout    = ""
        self.to_stdout      = ""

    @abc.abstractmethod
    def getCommand(self):
        pass

    def validate(self):
        if self.input_type == "": 
            self.error("In sorter implementation, input type is not specified!")

        elif self.input_type.lower() not in self.input_types:
            self.error("In sorter implementation, input type %s is not recognized!" % self.input_type)

        if self.output_type == "": 
            self.error("In sorter implementation, output type is not specified! Set the variable ")

        elif self.output_type.lower() not in self.output_types:
            self.error("In sorter implementation, output type %s is not recognized" % self.output_type)
    
        if self.from_stdout == "": 
            self.error("In sorter implementation, is the input from standard output?")

        if self.to_stdout == "": 
            self.error("In sorter implementation, is the output to standard output?")

    @staticmethod
    def todo():
        print("""
A complete sorter implementation should have the following:

Variables:
  config        Object      Config object containing options

  input_type    String      Choose from: %s
  output_type   String      Choose from: %s

  from_stdout   Boolean     Is the input from stdout?
  to_stdout     Boolean     Is the output to stdout?

Methods:
  __init__(self, config)        Constructor
        config      Config object

  getCommand(self)   Returns the command
""" % (", ".join(Converter.input_types), ", ".join(Converter.output_types)) )


