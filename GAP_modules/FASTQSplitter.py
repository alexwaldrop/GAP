from GAP_interfaces import Main
import os

class FASTQSplitter(Main):

    def __init__(self, config, file_path, type):
        Main.__init__(self, config)

        self.file_path  = file_path
        self.type       = type

    def byNrReads(self, nr_reads):
        
        if nr_reads <= 0:
            self.error("Cannot split a FASTQ file by %d reads!" % nr_reads)

        self.message("Splitting FASTQ file by %d reads." % nr_reads)

        split_count = 0
        with open(self.file_path) as f:
            
            done = False
          
            while not done:
    
                # Creating a new split file, considering the original fastq type
                split_count    += 1
                
                if self.type == "PE_R1":
                    split_filename  = "split_R1_%d.fastq" % split_count
                elif self.type == "PE_R2":
                    split_filename  = "split_R2_%d.fastq" % split_count
                elif self.type == "SE":
                    split_filename  = "split_%d.fastq" % split_count
                else:
                    self.warning("Unrecognized FASTQ file type '%s' in the pipeline. Default: Single-End.") 
                    split_filename  = "split_%d.fastq" % split_count

                split_filepath  = "%s/%s" % (self.general.temp_dir, split_filename)

                self.message("Writing to split file %s." % split_filename)

                # Writing to the new split file
                with open(split_filepath, "w") as out:

                    # Copying maximum nr_reads*4 lines
                    for i in range(nr_reads*4):
                        line = f.readline()
            
                        if line != "":
                            out.write(line)
                        else:
                            done = true
                            break

        self.message("Splitting FASTQ file has been completed.")

        return split_count

    def validate(self):
        
        if not os.path.isfile(self.file_path):
            self.error("Input file could not be found!")
