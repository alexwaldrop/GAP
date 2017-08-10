import logging
from Module import Module

class Trimmomatic (Module):
    def __init__(self, module_id):
        super(Trimmomatic, self).__init__(module_id)

        self.input_keys = ["R1", "R2", "trimmomatic", "java", "adapters", "nr_cpus", "mem"]
        self.output_keys = ["R1", "R2", "R1_unpair", "R2_unpair", "trim_report"]

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2")
        self.add_argument("trimmomatic",    is_required=True, is_resource=True)
        self.add_argument("java",           is_required=True, is_resource=True)
        self.add_argument("adapters",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

        # Optional trimmomatic arguments
        self.add_arguments("LEADING",                   is_required=True, default_value=5)
        self.add_arguments("TRAILING",                  is_required=True, default_value=5)
        self.add_arguments("MINLEN",                    is_required=True, default_value=36)
        self.add_arguments("SLIDINGWINDOW_SIZE",        is_required=True, default_value=4)
        self.add_arguments("SLIDINGWINDOW_QUAL",        is_required=True, default_value=10)
        self.add_arguments("keepBothReads",             is_required=True, default_value="true")
        self.add_arguments("seed_mismatches",           is_required=True, default_value=2)
        self.add_arguments("PALINDROME_CLIP_THRESHOLD", is_required=True, default_value=20)
        self.add_arguments("SIMPLE_CLIP_THRESHOLD",     is_required=True, default_value=7)
        self.add_arguments("MIN_ADAPTER_LEN",           is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):

        # Declare trimmed R1 output filename
        r1              = self.get_arguments("R1").get_value()
        ext             = "fastq.gz" if r1.endswith("gz") else "fastq"
        r1_trimmed_ext  = ".R1.trimmed.%s" % ext
        r1_trimmed_out  = self.generate_unique_file_name(split_name=split_name, extension=r1_trimmed_ext)
        self.add_output(platform, "R1", r1_trimmed_out)

        # Declare discarded R1 output filename
        r1_unpair_ext  = ".R1.unpair.%s" % ext
        r1_unpair_out  = self.generate_unique_file_name(split_name=split_name, extension=r1_unpair_ext)
        self.add_output(platform, "R1_unpair", r1_unpair_out)

        # Conditionally R2 output filenames
        r2 = self.get_arguments("R2").get_value()
        if r2 is not None:
            # Declare trimmed R2 output filename
            r2_trimmed_ext = ".R2.trimmed.%s" % ext
            r2_trimmed_out = self.generate_unique_file_name(split_name=split_name, extension=r2_trimmed_ext)
            self.add_output(platform, "R2", r2_trimmed_out)

            # Declare unpaired R2 output filename
            r2_unpair_ext = ".R2.unpair.%s" % ext
            r2_unpair_out = self.generate_unique_file_name(split_name=split_name, extension=r2_unpair_ext)
            self.add_output(platform, "R2_unpair", r2_unpair_out)

        # Declare trim report filename
        trim_report = self.generate_unique_file_name(split_name=split_name, extension=".trim_report.txt")
        self.add_output(platform, "trim_report", trim_report)

    def define_command(self, platform):
        # Generate command for running Trimmomatic

        # Get program options
        R1          = self.get_arguments("R1").get_value()
        R2          = self.get_arguments("R2").get_value()
        trimmomatic = self.get_arguments("trimmomatic").get_value()
        java        = self.get_arguments("java").get_value()
        adapters    = self.get_arguments("adapters").get_value()
        nr_cpus     = self.get_arguments("nr_cpus").get_value()
        mem         = self.get_arguments("mem").get_value()

        # Get output filenames
        R1_out          = self.get_output("R1")
        R1_unpair_out   = self.get_output("R1_unpair")
        trim_report     = self.get_output("trim_report")

        # Try to determine PHRED encoding from Fastq quality scores
        try:
            logging.info("Trimmomatic module determining PHRED encoding for fastq: %s" % R1)
            phred_encoding = Trimmomatic.__get_fastq_encoding(platform, R1, nr_cpus)

        except:
            logging.error("Unable to determine PHRED encoding for Trimmomatic module!")
            raise

        # Throw error if Phred is not Phred33 or Phred64
        if phred_encoding == "Solexa+64" or phred_encoding == "Unknown":
            logging.error("Unsupported phred encoding (%s) detected for fastq: %s" % (phred_encoding, R1))
            raise RuntimeError("Trimmomatic module: Unsupported PHRED encoding for FASTQ file!")

        # Set Trimmomatic Phred flag
        logging.info("PHRED encoding detected: %s" % phred_encoding)
        phred_option = "-phred33" if phred_encoding == "Phred+33" else "-phred64"

        # Set JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmp=%s" % (mem * 4 / 5, platform.get_workspace_dir("tmp"))

        # Set other Trimmomatic options
        leading             = self.get_arguments("LEADING").get_value()
        trailing            = self.get_arguments("TRAILING").get_value()
        minlen              = self.get_arguments("MINLEN").get_value()
        window_size         = self.get_arguments("SLIDINGWINDOW_SIZE").get_value()
        window_qual         = self.get_arguments("SLIDINGWINDOW_QUAL").get_value()
        keep_pair           = self.get_arguments("keepBothReads").get_value()
        mismatches          = self.get_arguments("seed_mismatches").get_value()
        pal_clip_thresh     = self.get_arguments("PALINDROME_CLIP_THRESHOLD").get_value()
        simple_clip_thresh  = self.get_arguments("SIMPLE_CLIP_THRESHOLD").get_value()
        min_adapt_len       = self.get_arguments("MIN_ADAPTER_LEN").get_value()

        steps = ["ILLUMINACLIP:%s:%d:%d:%d:%d:%s" % (adapters, mismatches, pal_clip_thresh,
                                                     simple_clip_thresh, min_adapt_len, keep_pair),
                 "LEADING:%d" % leading,
                 "TRAILING:%d" % trailing,
                 "SLIDINGWINDOW:%d:%d" % (window_size, window_qual),
                 "MINLEN:%d" % minlen]

        if R2 is not None:
            # Generate command for paired-end trimmomatic
            R2_out          = self.get_output("R2")
            R2_unpair_out   = self.get_output("R2_unpair")

            # Generating command
            cmd = "%s %s -jar %s PE -threads %d %s %s %s %s %s %s %s %s > %s 2>&1" % (
                java,
                jvm_options,
                trimmomatic,
                nr_cpus, phred_option,
                R1, R2, R1_out, R1_unpair_out, R2_out, R2_unpair_out,
                " ".join(steps),
                trim_report)
        else:
            # Generate command for single-end trimmomatic
            cmd = "%s %s -jar %s SE -threads %d %s %s %s %s > %s 2>&1" % (
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
            head_cmd = "pigz -p %d -d -k -c %s | head -n 400" % (nr_cpus, fastq)
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