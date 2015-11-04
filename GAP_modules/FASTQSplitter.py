from GAP_interfaces import Main

class FASTQSplitter(Main):

    def __init__(self, config, file_path):
        Main.__init__(self, config)

        self.file_path  = file_path

    def byNrReads(self, nr_reads):
        
        if nr_reads <= 0:
            self.error("Cannot split a FASTQ file by %d reads!" % nr_reads)

        self.message("Splitting FASTQ file by %d reads." % nr_reads)

        split_count = 0
        with open(self.file_path) as f:
            
            done = False
          
            while not done:
    
                # Creating a new split file
                split_count    += 1
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
