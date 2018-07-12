import os
from Modules import Module

class CNVkit(Module):
    def __init__(self, module_id, is_docker = False):
        super(CNVkit, self).__init__(module_id, is_docker)
        self.output_keys = ["cnr", "cns"]

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("cnvkit",             is_required=True, is_resource=True)
        self.add_argument("ref_cnn",            is_required=True)
        self.add_argument("method",             is_required=True, default_value="hybrid")
        self.add_argument("nr_cpus",            is_required=True, default_value=32)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self):
        #get bam file names from the sample sheet
        bam    = self.get_argument("bam")

        bam_file_ele    = os.path.splitext(bam)
        cnr_file_name   = "{0}.{1}".format(bam_file_ele[0], "cnr")
        cns_file_name   = "{0}.{1}".format(bam_file_ele[0], "cns")

        self.add_output("cnr", cnr_file_name)
        self.add_output("cns", cns_file_name)

    def define_command(self):

        # Get arguments
        bam                 = self.get_argument("bam")
        cnvkit              = self.get_argument("cnvkit")
        ref_cnn             = self.get_argument("ref_cnn")
        method              = self.get_argument("method")
        nr_cpus             = self.get_argument("nr_cpus")

        # Get current working dir
        working_dir = self.get_output_dir()

        #generate command line for cnvkit for hybrid (WES) method
        if method == "hybrid":
            cmd = "{0} batch {1} -r {2} --output-dir {3} -p {4} --drop-low-coverage --diagram --scatter".\
                format(cnvkit, bam, ref_cnn, working_dir, nr_cpus)

        # generate command line for cnvkit for WGS method
        elif method == "wgs":
            cmd = "{0} batch {1} -r {2} --output-dir {3} -p {4} --method {5} --drop-low-coverage --diagram --scatter".\
                format(cnvkit, bam, ref_cnn, working_dir, nr_cpus, method)

        else:
            raise NotImplementedError("Method {0} is not implemented in CNVKit module.".format(method))

        cmd = "{0} !LOG3!".format(cmd)

        return cmd