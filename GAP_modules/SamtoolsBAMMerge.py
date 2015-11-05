from GAP_interfaces import Main

class SamtoolsBAMMerge(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.sorted_input = False
        self.nr_splits    = 0

    def merge(self):
        bam_splits = ["split_%d.bam" % i for i in range(nr_splits)]
        
        if self.sorted_input:
            return "samtools merge out.bam %s" % " ".join(bam_splits)
        else:
            return "samtools cat -o out.bam %s" % " ".join(bam_splits)

    def validate(self):
        if self.nr_splits == 0:
            self.error("Number of splits was not set before merging!")
