import logging
import time

from System.Workers import NodeWorker

class PipelineWorker(object):

    def __init__(self, pipeline):

        # Initialize the pipeline variables
        self.pipeline       = pipeline
        self.graph          = self.pipeline.get_graph()
        self.resource_kit   = self.pipeline.get_resource_kit()
        self.sample_set     = self.pipeline.get_sample_set()
        self.platform       = self.pipeline.get_platform()

        # Initialize the node workers set
        self.node_workers = {}
        self.__init_node_workers()

        # Initialize completed nodes list
        self.completed_nodes = []

        # Initialize the final output paths
        self.final_output = {}

    def __init_node_workers(self):

        # Get pipeline graph nodes
        nodes = self.graph.get_nodes()

        # Create NodeWorker objects
        for node_id, node_obj in nodes.iteritems():
            self.node_workers[node_id] = NodeWorker(self.platform, node_obj)

    def __run_node_workers(self):

        # Get pipeline graph data
        adj_list = self.graph.get_adjacency_list()

        # Initialize completion flag and finalized node list
        done = False
        completed = []

        while not done:

            # Assume all nodes are finalized
            done = True

            # Check all node workers if they have completed
            for node_id, node_worker in self.node_workers.iteritems():

                # Skip already completed nodes
                if node_id in self.completed_nodes:
                    continue

                # Check if node worker is done
                if node_worker.is_done():

                    # Finalize the node worker
                    node_worker.finalize()

                    # Log the completion
                    logging.info("Node '%s' has finished!" % node_id)

                    # Mark as completed
                    self.completed_nodes.append(node_id)

                    # Obtain the final output
                    self.final_output[node_id] = node_worker.get_final_output()

                    continue

                # Check if node is still working
                if node_worker.is_alive():
                    continue

                # Node has not been processed yet. Check if all the required nodes are complete
                ready = True
                for required_node_id in adj_list[node_id]:
                    if required_node_id not in completed:
                        ready = False
                        break

                # At least one of node's requirements is not complete, so this node cannot be processed yet
                if not ready:
                    continue

                # All node's requirements are complete, so the node can be processed
                # Get input data from all the required nodes
                workers_output = []
                for required_node_id in adj_list[node_id]:
                    workers_output.append( self.node_workers[required_node_id].get_output() )

                # Convert the input data from a list of dictionaries to a dictionary of lists
                node_input = {}
                if len(workers_output):
                    for key in workers_output[0]:
                        node_input[key] = [ worker_output[key] for worker_output in workers_output ]

                # Set the input to the node worker
                node_worker.set_input( node_input=node_input,
                                       sample_input=self.sample_set.get_data(),
                                       resource_input=self.resource_kit.get_resources())

                # Start the node worker
                node_worker.start()

            # Sleeping for 5 seconds before checking again
            time.sleep(5)

    def run(self):

        try:
            self.__run_node_workers()
        finally:
            self.__finalize()

    def __copy_final_output(self):

        # Get graph nodes
        nodes = self.graph.get_nodes()

        # Return the final output for all node workers
        for node_id, final_output_paths in self.final_output.iteritems():

            # Get the main module name
            main_module_name = nodes[node_id].main_module.get_ID()

            # Transfer all final output files
            for path_key, paths in final_output_paths.iteritems():

                # Store more than one file in a sub-directory named as the main module
                if len(paths) > 1:
                    for path in paths:
                        self.platform.return_output(path, sub_dir=main_module_name)
                else:
                    self.platform.return_output(paths)

    def __copy_logs(self):

        # Get the workspace log directory
        log_dir = self.platform.get_workspace_dir(sub_dir="log")

        # Transfer the log directory as final output
        self.platform.return_output(log_dir, log_transfer=False)

    def __finalize(self):

        # Copy the final output paths
        self.__copy_final_output()

        # Copy the logs directory
        self.__copy_logs()
