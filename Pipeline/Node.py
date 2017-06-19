import importlib
import logging

from Util import Thread
from IO import PipelineFile, PlatformFileSet

class Node(Thread):

    class SplitServer(Thread):

        def __init__(self, instance_name, platform, job_name, cmd, **kwargs):
            super(Node.SplitServer, self).__init__()

            self.platform       = platform
            self.instance_name  = instance_name
            self.instance       = None

            self.job_name       = job_name
            self.cmd            = cmd

            self.kwargs         = kwargs

            # Setting up the error message in case the thread raises an exception
            self.set_error_message("(%s) Exception in executing thread" % self.instance_name)

        def task(self):

            # Creating split server
            self.platform.create_instance(self.instance_name, **self.kwargs)
            self.instance = self.platform.instances[self.instance_name]

            # Waiting for split server to be created
            self.instance.wait_process("create")

            # Running the command on instance
            self.instance.run_command(self.job_name, self.cmd)

            # Waiting for the job to finish
            self.instance.wait_process(self.job_name)

            # If no exceptions were raised and we reach this point
            self.instance.destroy()

            # Waiting for split server to be destroyed
            self.instance.wait_process("destroy")

    def __init__(self, platform, **kwargs):
        super(Node, self).__init__()

        # Setting base variables
        self.platform       = platform
        self.config         = self.platform.get_config()
        self.pipeline_data  = self.platform.get_pipeline_data()

        # Obtaining configuration
        self.kwargs             = kwargs
        self.tool_id            = self.kwargs.pop("tool_id")
        self.module_name        = self.kwargs.pop("module")
        self.final_output_types = self.kwargs.pop("final_output",   list())
        self.is_split_mode      = self.kwargs.pop("split",          self.config["general"]["split"])
        self.extra_args         = self.kwargs

        # Input data for the entire node
        self.input_data     = None

        # Input files passed from upstream nodes
        self.input_files    = None

        # Output files produced by this node
        self.output_files   = PlatformFileSet()

        # Output data from all the sections of the node
        self.split_outputs      = None
        self.main_outputs       = None
        self.merge_outputs      = None

        # Output data to be kept upon pipeline completion
        self.final_outputs      = None

        # Setting up the error message in case the thread raises an exception
        self.set_error_message("Exception in executing node with module '%s'" % self.module_name)

        # Importing main module
        try:
            self.main = Node.init_module(self.module_name, is_tool=True)
            self.main_obj = self.main["class"](self.platform, self.tool_id)
        except ImportError:
            logging.error("Module %s cannot be imported!" % self.module_name)
            exit(1)

        # Identify if the node is running in split mode
        self.is_split_mode = self.main_obj.can_split and self.is_split_mode

        # Importing splitter and merger:
        if self.is_split_mode:

            try:
                self.split = Node.init_module(self.main_obj.splitter, is_splitter=True)
                self.split_obj = self.split["class"](self.platform, self.tool_id, main_module_name=self.module_name)
            except ImportError:
                logging.error("Module %s cannot be imported!" % self.main_obj.splitter)
                exit(1)

            try:
                self.merge = Node.init_module(self.main_obj.merger, is_merger=True)
                self.merge_obj = self.merge["class"](self.platform, self.tool_id, main_module_name=self.module_name)
            except ImportError:
                logging.error("Module %s cannot be imported!" % self.main_obj.merger)
                exit(1)

    @staticmethod
    def init_module(module_name, is_tool=False, is_splitter=False, is_merger=False):

        d = dict()
        d["module_name"] = module_name

        if is_tool:
            d["module"] = importlib.import_module("Modules.Tools.%s" % d["module_name"])
        elif is_splitter:
            d["module"] = importlib.import_module("Modules.Splitters.%s" % d["module_name"])
        elif is_merger:
            d["module"] = importlib.import_module("Modules.Mergers.%s" % d["module_name"])
        else:
            logging.error("Module %s could not be imported! Specify whether module is tool, splitter, or merger!")

        d["class_name"] = d["module"].__main_class__
        d["class"] = d["module"].__dict__[d["class_name"]]

        return d

    def task(self):

        if self.is_split_mode:
            self.run_split()
        else:
            self.run_normal()

        # Create PipelineFileSet that can be used as input for downstream nodes
        #self.prepare_output()

        # Register node output files to be saved upon pipeline completion with the platform
        self.register_final_output()

    def run_split(self):

        # Creating job names
        split_job_name  = "%s_split" % self.module_name
        main_job_name   = lambda splt_id: "%s_%d" % (self.module_name, splt_id)
        merge_job_name  = "%s_merge" % self.module_name

        # Running the splitter
        cmd = self.split_obj.generate_command(**self.input_data)

        if cmd is not None:
            self.platform.main_instance.run_command(split_job_name, cmd)
            self.platform.main_instance.wait_process(split_job_name)

        # Obtaining the splitter outputs
        self.split_outputs = self.split_obj.get_output()

        self.main_outputs = list()

        # Creating the split server threads
        split_servers = dict()
        for split_id, args in enumerate(self.split_outputs):

            # Obtaining main command
            cmd = self.main_obj.generate_command(split_id=split_id, **args)

            # Obtaining the main output
            self.main_outputs.append(self.main_obj.get_output())

            # Checking if there is command to run
            if cmd is not None:

                # Generating split server name
                server_name = self.platform.generate_split_instance_name(self.tool_id,
                                                                         self.module_name,
                                                                         split_id=split_id)
                # Obtaining required resources
                kwargs = dict()
                kwargs["nr_cpus"] = self.main_obj.get_nr_cpus()
                kwargs["mem"] = self.main_obj.get_mem()

                # Creating SplitServer object
                split_servers[server_name] = self.SplitServer(server_name, self.platform, main_job_name(split_id), cmd, **kwargs)

                # Starting split server work
                split_servers[server_name].start()

        # Waiting for all the split processes to finish
        done = False
        while not done:

            # Assume all server_threads have finished
            done = True

            # Check each server if it has finished
            for server_thread in split_servers.itervalues():
                if server_thread.is_done():
                    server_thread.finalize()
                else:
                    done = False

        # Convert the input to a dictionary of lists
        merge_input = dict()
        for key in self.main_outputs[0]:
            merge_input[key] = [ main_output[key] for main_output in self.main_outputs]

        # Running the merger
        cmd = self.merge_obj.generate_command(**merge_input)
        self.platform.main_instance.run_command(merge_job_name, cmd)
        self.platform.main_instance.wait_process(merge_job_name)

        # Obtaining the merger outputs
        self.merge_outputs = self.merge_obj.get_output()

    def run_normal(self):

        # Obtaining main command
        cmd = self.main_obj.generate_command(**self.input_data)

        # Obtaining output that will be saved at the end of the pipeline
        self.main_outputs = self.main_obj.get_output()

        if cmd is None:
            logging.debug("Module %s has generated no command." % self.module_name)
            return None

        self.platform.main_instance.run_command(self.module_name, cmd)
        self.platform.main_instance.wait_process(self.module_name)

    def register_final_output(self):
        # Register final output files with platform to be returned upon completion

        output_file_types   = self.final_output_types
        files_to_return     = []

        while len(output_file_types) > 0:
            # Get next file type to return
            output_file_type = output_file_types.pop()

            # Add files matching the output_file_type
            if self.is_split_mode:

                # Get outputs from merge, main, and split modules
                merge_output = self.merge_outputs.filter_by_attribute(attribute="file_type",
                                                                      value=output_file_type)

                main_output  = self.main_outputs.filter_by_attribute(attribute="file_type",
                                                                      value=output_file_type)

                split_output = self.split_outputs.filter_by_attribute(attribute="file_type",
                                                                      value=output_file_type)

                # Add output files only from the 'highest' possible level for an output file type
                if len(merge_output) > 0:
                    files_to_return.extend(merge_output)
                elif len(main_output) > 0:
                    files_to_return.extend(main_output)
                else:
                    files_to_return.extend(split_output)

            else:
                files_to_return.extend(self.main_outputs.filter_by_attribute(attribute="file_type",
                                                                             value=output_file_type))



        # Get every key that is first found in the merger output or the splits output
        for key in self.final_output_types:

            # Add all the paths to the final output depending on the running method
            if self.is_split_mode:

                # Search the key in the merger output
                if key in self.merge_outputs:
                    self.pipeline_data.add_final_output(tool_id=self.tool_id,
                                                        module_name=self.module_name,
                                                        output_file_type=key,
                                                        output_file=self.merge_outputs[key])
                # Search the key in the main object output
                elif key in self.main_outputs[0]:
                    for main_output in self.main_outputs:
                        self.pipeline_data.add_final_output(tool_id=self.tool_id,
                                                            module_name=self.module_name,
                                                            output_file_type=key,
                                                            output_file=main_output[key])
                # Search the key in the splitter output
                elif key in self.split_outputs[0]:
                    for split_output in self.split_outputs:
                        self.pipeline_data.add_final_output(tool_id=self.tool_id,
                                                            module_name=self.module_name,
                                                            output_file_type=key,
                                                            output_file=split_output[key])
            else:

                # Search the key in the main object output
                if key in self.main_outputs:

                    self.pipeline_data.add_final_output(tool_id=self.tool_id,
                                                        module_name=self.module_name,
                                                        output_file_type=key,
                                                        output_file=self.main_outputs[key])

    def get_module_name(self):
        return self.module_name

    def get_output_types(self):
        if self.is_split_mode:
            return self.merge_obj.define_output()
        else:
            return self.main_obj.define_output()

    def get_output(self):
        if self.is_split_mode:
            return self.merge_outputs
        else:
            return self.main_outputs

    def get_final_output_types(self):
        return self.final_output_types

    def is_split_mode(self):
        return self.is_split_mode

    def set_resources(self):
        #derp
        pass

    def set_input(self, input_list):

        # Convert from list of dictionary to dictionary of lists
        input_data = dict()
        for input_dict in input_list:
            for key in input_dict:

                # Initialize the input_data
                if key not in input_data:
                    input_data[key] = list()

                # Add value to the input data
                input_data[key].append( input_dict[key] )

        # Each list of one element, will be converted to the element
        self.input_data = dict()
        for key in input_data:
            if len(input_data[key]) == 1:
                self.input_data[key] = input_data[key][0]
            else:
                self.input_data[key] = input_data[key]

        logging.debug("Module %s has received the following input: %s" % (self.module_name, self.input_data))
