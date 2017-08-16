from copy import deepcopy

from System.Workers import Thread
from System.Workers import ModuleWorker

class NodeWorker(Thread):

    def __init__(self, platform, node_obj):

        # Get the name of the node that is processed
        self.node_id = node_obj.get_ID()

        # Initialize the thread
        err_msg = "NodeWorker for %s has stopped working!" % self.node_id
        super(NodeWorker, self).__init__(err_msg)

        # Get the necessary variables
        self.platform = platform
        self.node_obj = node_obj
        self.split_mode = self.node_obj.is_split_mode()

        # Initialize the modules
        self.split_module   = self.node_obj.split_module
        self.main_module    = self.node_obj.main_module
        self.merge_module   = self.node_obj.merge_module

        # Initialize the input data
        self.input_data     = {}

        # Initialize the output data
        self.split_output   = None
        self.main_output    = None
        self.merge_output   = None

    def __run_split(self):

        #region STEP 1: Split the input data

        # Generate job name for splitter worker
        split_job_name  = self.split_module.get_ID()

        # Create a ModuleWorker for the splitter
        split_worker    = ModuleWorker(self.platform, self.split_module, split_job_name)

        # Set input to the worker
        split_worker.set_input(**self.input_data)

        # Start splitter work and wait for it to finish
        split_worker.start()
        split_worker.finalize()

        # Obtain the splits from the splitter
        self.split_output = split_worker.get_output()

        #endregion

        #region STEP 2: Process each split in parallel

        # Create a dictionary of main workers
        main_workers = {}

        # Process the splits
        for split_name, split_args in self.split_output.iteritems():

            # Generate the job name for the worker
            main_job_name = "%s_%s" % (self.main_module.get_ID(), split_name)

            # Create a ModuleWorker for each split
            main_workers[main_job_name] = ModuleWorker(self.platform,
                                                       deepcopy(self.main_module),
                                                       main_job_name,
                                                       split_name=split_name)

            # Set input to the worker
            main_workers[main_job_name].set_input(module_input=split_args, **self.input_data)
            # Start processing the split
            main_workers[main_job_name].start()

        # Check the status of the main_workers and wait for them to be finished
        all_done = False
        while not all_done:

            # Assume all workers are done
            all_done = True

            # Check if all workers are actually done
            for worker in main_workers.itervalues():
                if worker.is_done():
                    worker.finalize()
                else:
                    all_done = False

        # Obtain the main workers output
        main_workers_output = [ worker.get_output() for worker in main_workers ]

        # Convert the outputs from a list of dictionaries to a dictionary of lists
        self.main_output = {}
        for key in main_workers_output[0]:
            self.main_output[key] = [ worker_output[key] for worker_output in main_workers_output ]

        #endregion

        #region STEP 3: Merge the outputs

        # Generate job name for merger worker
        merge_job_name = self.merge_module.get_ID()

        # Create a ModuleWorker for the merger
        merge_worker    = ModuleWorker(self.platform, self.merge_module, merge_job_name)

        # Set input to the worker
        merge_worker.set_input(module_input=self.main_output, **self.input_data)

        # Start the merger and wait for it to finish
        merge_worker.start()
        merge_worker.finalize()

        # Save the merger output
        self.merge_output = merge_worker.get_output()

        #endregion

    def __run_normal(self):

        # Generate job name for the main worker
        main_job_name   = self.main_module.get_ID()

        # Create a ModuleWorker for the main module
        main_worker     = ModuleWorker(self.platform, self.main_module, main_job_name)

        # Set input to the worker
        main_worker.set_input(**self.input_data)

        # Start the worker and wait for it to finish
        main_worker.start()
        main_worker.finalize()

        # Save the main worker output
        self.main_output = main_worker.get_output()

    def task(self):
        if self.split_mode:
            self.__run_split()
        else:
            self.__run_normal()

    def set_input(self, **kwargs):

        # Get the input values
        self.input_data["node_input"]       = kwargs.get("node_input",      [])
        self.input_data["sample_input"]     = kwargs.get("sample_input",    [])
        self.input_data["resource_input"]   = kwargs.get("resource_input",  [])

        # Obtain the config input from the node object
        self.input_data["config_input"]     = self.node_obj.get_config_input()

    def get_output(self):
        if self.split_mode:
            return self.node_obj.merge_module.get_output()
        else:
            return self.node_obj.main_module.get_output()

    def get_final_output(self):

        # Get the list of final output keys
        final_output_keys = self.node_obj.get_final_output_keys()

        # Initialize final output data
        final_output = {}

        # Define the list and order of the output types that will be searched
        if self.split_mode:
            output_types = [self.merge_output, self.main_output, self.split_output]
        else:
            output_types = [self.main_output]

        # Get every key that is first found in the provided list of output types
        for key in final_output_keys:
            for output_type in output_types:
                if key in output_type:
                    final_output[key] = output_type[key]
                    break

        return final_output
