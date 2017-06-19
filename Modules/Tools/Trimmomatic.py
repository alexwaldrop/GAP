import logging
from GAP_interfaces import Tool

__main_class__ = "Trimmomatic"

class Trimmomatic(Tool):

    def __init__(self, platform, tool_id):
        super(Trimmomatic, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        self.input_keys     = ["R1", "R2"]
        self.output_keys    = ["R1", "R1_unpair", "R2", "R2_unpair", "trim_report"]

        self.req_tools      = ["trimmomatic", "java"]
        self.req_resources  = ["adapters"]

    def get_command(self, **kwargs):

        # Obtaining the arguments
        R1                 = kwargs.get("R1",              None)
        R2                 = kwargs.get("R2",              None)
        nr_cpus            = kwargs.get("nr_cpus",         self.nr_cpus)
        mem                = kwargs.get("mem",             self.mem)

        # Determine PHRED encoding from Fastq quality scores
        phred_encoding = self.get_fastq_encoding(R1, nr_cpus)
        # Throw error if Phred is not Phred33 or Phred64
        if phred_encoding == "Solexa+64" or phred_encoding == "Unknown":
            msg = "Unsupported phred encoding (%s) detected for Trimmomatic using file: %s" % (phred_encoding, R1)
            logging.error(msg)
            exit(1)

        logging.info("PHRED encoding detected: %s" % phred_encoding)

        # Set Trimmomatic Phred flag
        phred_option = "-phred33" if phred_encoding == "Phred+33" else "-phred64"

        # Set other Trimmomatic options
        steps       = [ "ILLUMINACLIP:%s:2:20:7:1:true" % self.resources["adapters"],
                        "LEADING:5",
                        "TRAILING:5",
                        "SLIDINGWINDOW:4:10",
                        "MINLEN:36" ]

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmp=%s" % (mem*4/5, self.tmp_dir)

        # Generating command
        trim_cmd = "%s %s -jar %s PE -threads %d %s %s %s %s %s %s %s %s > %s 2>&1" % (
            self.tools["java"], jvm_options, self.tools["trimmomatic"], nr_cpus, phred_option, R1, R2,
            self.output["R1"],
            self.output["R1_unpair"],
            self.output["R2"],
            self.output["R2_unpair"],
            " ".join(steps),
            self.output["trim_report"])

        return trim_cmd

    def init_output_file_paths(self, **kwargs):

        self.generate_output_file_path(output_key="R1",
                                       extension="R1.trimmed.fastq")

        self.generate_output_file_path(output_key="R1_unpair",
                                       extension="R1.trimmed_unpaired.fastq")

        self.generate_output_file_path(output_key="R2",
                                       extension="R2.trimmed.fastq")

        self.generate_output_file_path(output_key="R2_unpair",
                                       extension="R2.trimmed_unpaired.fastq")

        self.generate_output_file_path(output_key="trim_report",
                                       extension=".trim_report.txt")

    def get_fastq_encoding(self, fastq, nr_cpus):
        # Determine phred quality encoding from FASTQ
        # Algorithm taken from http://onetipperday.sterding.com/2012/10/code-snip-to-decide-phred-encoding-of.html
        if fastq.endswith(".gz"):
            head_cmd = "pigz -p %d -d -k -c %s | head -n 400" % (nr_cpus, fastq)
        else:
            head_cmd = "cat %s | head -n 400" % fastq

        get_qual_score_cmd = "awk '{if(NR%4==0) printf(\"%s\",$0);}' | od -A n -t u1"
        cmd = "%s | %s" % (head_cmd, get_qual_score_cmd)

        main_instance = self.platform.get_main_instance()
        main_instance.run_command("fastq_phred", cmd, log=False)
        out, err = main_instance.get_proc_output("fastq_phred")

        if err != "":
            err_msg = "Could not determine the FASTQ file phred encoding!"
            err_msg += "\nThe following command was run: \n  %s " % cmd
            err_msg += "\nThe following error appeared: \n  %s" % err
            logging.error(err_msg)
            exit(1)

        # Parse Phred scores
        scores = list()
        for line in out.split("\n"):
            line = line.split()
            for pos in line:
                if (pos != "") and (pos != "*"):
                    scores.append(int(pos))

        # Determine encoding from scores
        min_qual = min(scores)
        max_qual = max(scores)
        if max_qual <= 75 and min_qual < 59:
            return "Phred+33"
        elif max_qual > 73 and min_qual >=64:
            return "Phred+64"
        elif min_qual >= 59 and min_qual < 64 and max_qual > 73:
            return "Solexa+64"
        else:
            return "Unknown"
