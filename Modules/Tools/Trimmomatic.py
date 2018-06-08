import logging

from Modules import Module

class Trimmomatic (Module):
    def __init__(self, module_id):
        super(Trimmomatic, self).__init__(module_id)
        self.output_keys = ["R1", "R2", "R1_unpair", "R2_unpair", "trim_report"]

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2")
        self.add_argument("phred_encoding", is_required=True, default_value="Phred+33")
        self.add_argument("trimmomatic",    is_required=True, is_resource=True)
        self.add_argument("java",           is_required=True, is_resource=True)
        self.add_argument("adapters",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value="MAX")
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 1")

        # Optional trimmomatic arguments
        self.add_argument("LEADING",                   is_required=True, default_value=5)
        self.add_argument("TRAILING",                  is_required=True, default_value=5)
        self.add_argument("MINLEN",                    is_required=True, default_value=36)
        self.add_argument("SLIDINGWINDOW_SIZE",        is_required=True, default_value=4)
        self.add_argument("SLIDINGWINDOW_QUAL",        is_required=True, default_value=10)
        self.add_argument("keepBothReads",             is_required=True, default_value="true")
        self.add_argument("seed_mismatches",           is_required=True, default_value=2)
        self.add_argument("PALINDROME_CLIP_THRESHOLD", is_required=True, default_value=20)
        self.add_argument("SIMPLE_CLIP_THRESHOLD",     is_required=True, default_value=7)
        self.add_argument("MIN_ADAPTER_LEN",           is_required=True, default_value=1)

    def define_output(self):

        # Declare trimmed R1 output filename
        r1_trimmed_out  = self.generate_unique_file_name(extension=".R1.trimmed.fastq")
        self.add_output("R1", r1_trimmed_out)

        # Declare discarded R1 output filename
        r1_unpair_out  = self.generate_unique_file_name(extension=".R1.unpair.fastq")
        self.add_output("R1_unpair", r1_unpair_out)

        # Conditionally R2 output filenames
        if self.get_argument("R2") is not None:
            # Declare trimmed R2 output filename
            r2_trimmed_out = self.generate_unique_file_name(extension=".R2.trimmed.fastq")
            self.add_output("R2", r2_trimmed_out)

            # Declare unpaired R2 output filename
            r2_unpair_out = self.generate_unique_file_name(extension=".R2.unpair.fastq")
            self.add_output("R2_unpair", r2_unpair_out)

        # Declare trim report filename
        trim_report = self.generate_unique_file_name(extension=".trim_report.txt")
        self.add_output("trim_report", trim_report)

    def define_command(self):
        # Generate command for running Trimmomatic

        # Get program options
        R1              = self.get_argument("R1")
        R2              = self.get_argument("R2")
        trimmomatic     = self.get_argument("trimmomatic")
        phred_encoding  = self.get_argument("phred_encoding")
        java            = self.get_argument("java")
        adapters        = self.get_argument("adapters")
        nr_cpus         = self.get_argument("nr_cpus")
        mem             = self.get_argument("mem")

        # Get output filenames
        R1_out          = self.get_output("R1")
        R1_unpair_out   = self.get_output("R1_unpair")
        trim_report     = self.get_output("trim_report")

        # Throw error if Phred is not Phred33 or Phred64
        if phred_encoding == "Solexa+64" or phred_encoding == "Unknown":
            logging.error("Unsupported phred encoding (%s) detected for fastq: %s" % (phred_encoding, R1))
            raise RuntimeError("Trimmomatic module: Unsupported PHRED encoding for FASTQ file!")

        # Set Trimmomatic Phred flag
        logging.info("PHRED encoding detected: %s" % phred_encoding)
        phred_option = "-phred33" if phred_encoding == "Phred+33" else "-phred64"

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmp=%s" % (mem * 4 / 5, "/tmp/")

        # Set other Trimmomatic options
        leading             = self.get_argument("LEADING")
        trailing            = self.get_argument("TRAILING")
        minlen              = self.get_argument("MINLEN")
        window_size         = self.get_argument("SLIDINGWINDOW_SIZE")
        window_qual         = self.get_argument("SLIDINGWINDOW_QUAL")
        keep_pair           = self.get_argument("keepBothReads")
        mismatches          = self.get_argument("seed_mismatches")
        pal_clip_thresh     = self.get_argument("PALINDROME_CLIP_THRESHOLD")
        simple_clip_thresh  = self.get_argument("SIMPLE_CLIP_THRESHOLD")
        min_adapt_len       = self.get_argument("MIN_ADAPTER_LEN")

        steps = ["ILLUMINACLIP:%s:%s:%s:%s:%s:%s" % (adapters, mismatches, pal_clip_thresh,
                                                     simple_clip_thresh, min_adapt_len, keep_pair),
                 "LEADING:%s" % leading,
                 "TRAILING:%s" % trailing,
                 "SLIDINGWINDOW:%s:%s" % (window_size, window_qual),
                 "MINLEN:%s" % minlen]

        if R2 is not None:
            # Generate command for paired-end trimmomatic
            R2_out          = self.get_output("R2")
            R2_unpair_out   = self.get_output("R2_unpair")

            # Generating command
            cmd = "%s %s -jar %s PE -threads %s %s %s %s %s %s %s %s %s > %s 2>&1" % (
                java,
                jvm_options,
                trimmomatic,
                nr_cpus, phred_option,
                R1, R2, R1_out, R1_unpair_out, R2_out, R2_unpair_out,
                " ".join(steps),
                trim_report)
        else:
            # Generate command for single-end trimmomatic
            cmd = "%s %s -jar %s SE -threads %s %s %s %s %s > %s 2>&1" % (
                java,
                jvm_options,
                trimmomatic,
                nr_cpus, phred_option,
                R1, R1_out,
                " ".join(steps),
                trim_report)
        return cmd

    @staticmethod
    def __get_fastq_encoding(platform, fastq, nr_cpus):
        # Determine phred quality encoding from FASTQ
        # Algorithm taken from http://onetipperday.sterding.com/2012/10/code-snip-to-decide-phred-encoding-of.html
        if fastq.endswith(".gz"):
            head_cmd = "pigz -p %s -d -k -c %s | head -n 400" % (nr_cpus, fastq)
        else:
            head_cmd = "cat %s | head -n 400" % fastq

        get_qual_score_cmd = "awk '{if(NR%4==0) printf(\"%s\",$0);}' | od -A n -t u1"
        cmd = "%s | %s" % (head_cmd, get_qual_score_cmd)
        out, err = platform.run_quick_command("fastq_phred", cmd)

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