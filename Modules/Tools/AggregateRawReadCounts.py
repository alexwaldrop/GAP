import os
from Modules import Module

class AggregateRawReadCounts(Module):
    def __init__(self, module_id):
        super(AggregateRawReadCounts, self).__init__(module_id)

        self.input_keys = ["sample_name", "raw_read_counts", "nr_cpus", "mem"]

        self.output_keys = ["aggregated_raw_read_counts"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("raw_read_counts",    is_required=True)
        self.add_argument("aggregate",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):

        # Declare unique file name
        output_file_name = self.generate_unique_file_name(split_name=split_name,
                                                                 extension=".txt")

        self.add_output(platform, "aggregated_raw_read_counts", output_file_name)

    def define_command(self, platform):

        # Get arguments
        samples                     = self.get_arguments("sample_name").get_value()
        raw_read_counts             = self.get_arguments("raw_read_counts").get_value()

        #get the aggregate script to run
        aggregate_script = self.get_arguments("aggregate").get_value()

        # Get current working dir
        working_dir = platform.get_workspace_dir()

        # Generate output file name prefix for STAR
        input_file = os.path.join(working_dir, "{0}".format("sample_info.txt"))

        #get the output file and make appropriate path for it
        output_file = self.get_output("aggregated_raw_read_counts")

        #iterate through all the samples to create a sample info file for Rscript
        for index in range(len(samples)):
            if index == 0:
                cmd = 'echo -e "samples\\tfiles" > {0}'.format(input_file)
                platform.run_quick_command("make_sample_info_file", cmd)
            cmd = 'echo -e "{0}\\t{1}" >> {2}'.format(samples[index], raw_read_counts[index], input_file)
            platform.run_quick_command("make_sample_info_file", cmd)

        #generate command line for Rscript
        cmd = "sudo Rscript --vanilla {0} -f {1} -o {2} !LOG3!".format(aggregate_script, input_file, output_file)

        return cmd