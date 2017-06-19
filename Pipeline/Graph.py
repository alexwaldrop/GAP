import time
import logging

from Node import Node

class Graph(object):

    def __init__(self, platform):

        self.platform       = platform
        self.config         = self.platform.get_config()
        self.pipeline_data  = self.platform.get_pipeline_data()

        self.requires   = dict()
        self.nodes      = dict()

        self.generate_graph()

    def generate_graph(self):

        logging.info("Generating the graph of tools.")

        for tool_id in self.config["tools"]:

            tool_data = self.config["tools"][tool_id]

            if not isinstance(tool_data, dict):
                continue

            # Add tool_id in the tool_data
            tool_data["tool_id"] = tool_id

            self.requires[tool_id] = tool_data.pop("input_from")
            self.nodes[tool_id] = Node(self.platform, **tool_data)

    def check_nodes(self):

        has_errors = False

        # Checking input/output keys
        for tool_id in self.nodes:

            # Obtaining information about the node
            module_name         = self.nodes[tool_id].get_module_name()
            final_output_keys   = self.nodes[tool_id].define_final_output()

            # Identifying all the input keys
            input_keys = list()
            for required_tool_id in self.requires[tool_id]:
                if required_tool_id == "main_input":
                    input_keys.extend(self.pipeline_data.get_main_input_keys())
                else:
                    input_keys.extend(self.nodes[required_tool_id].define_output())

            logging.info("Checking I/O for module %s." % module_name)

            # Testing the input keys and the final_output keys
            input_err = self.nodes[tool_id].check_input(input_keys)
            output_err = self.nodes[tool_id].check_output(final_output_keys)
            for error in input_err, output_err:
                if error is not None:
                    has_errors = True
                    logging.error("For the %s (%s), the following I/O error appeared: %s " % (tool_id, module_name, error))

        # Checking tools and resources requirements
        for tool_id in self.nodes:

            # Obtaining information about the node
            module_name         = self.nodes[tool_id].module_name

            # Identifying if any required keys are not found
            not_found = self.nodes[tool_id].check_requirements()

            # Checking if all the required tools are provided
            if not_found["tools"]:
                has_errors = True
                logging.error(
                    "For the %s (%s), the following tools are required, but are not found in the config: %s " %
                    (tool_id, module_name, " ".join(not_found["tools"])))

            # Checking if all the required resources are provided
            if not_found["resources"]:
                has_errors = True
                logging.error(
                    "For the %s (%s), the following resources are required, but are not found in the config: %s " %
                    (tool_id, module_name, " ".join(not_found["resources"])))

        if has_errors:
            raise IOError("One or more modules have I/O errors. Please check the error messages above!")

    def run(self):

        done = False
        completed = list()

        while not done:

            done = True

            for tool_id in self.nodes:

                # Check if tool was marked as completed
                if tool_id in completed:
                    continue

                # Check if tool has finished running
                if self.nodes[tool_id].is_done():
                    self.nodes[tool_id].finalize()
                    logging.info("Module '%s' has finished!" % self.nodes[tool_id].get_module_name())
                    completed.append(tool_id)
                    continue

                # Set loop as not done
                done = False

                # Check if tool is still running
                if self.nodes[tool_id].is_alive():
                    continue

                # Check if all the required tools are complete
                ready = True
                for required_tool_id in self.requires[tool_id]:
                    if required_tool_id != "main_input" and required_tool_id not in completed:
                        ready = False
                        break

                if not ready:
                    continue

                # Generating input data
                input_data = list()
                for required_tool_id in self.requires[tool_id]:
                    if required_tool_id == "main_input":
                        input_data.extend( self.pipeline_data.get_main_input() )
                    else:
                        input_data.append( self.nodes[required_tool_id].get_output() )

                # Launching the tool
                self.nodes[tool_id].set_input(input_data)
                self.nodes[tool_id].start()

            # Sleeping for 5 seconds before checking again
            time.sleep(5)
