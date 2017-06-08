from PipelineFile import PipelineFile

class OutputFile(PipelineFile):
    # Extends PipelineFile to hold information related to output file types
    def __init__(self, **kwargs):
        # Init super class
        super(OutputFile, self).__init__(**kwargs)

        # Boolean flag for whether output file is to be transferred to output directory upon pipeline completion
        self.marked_for_return = kwargs.get("return_on_complete", False)

    def mark_for_return(self):
        self.makred_for_return = True

    def unmark_for_return(self):
        self.marked_for_return = False

    def is_marked_for_return(self):
        return self.marked_for_return



