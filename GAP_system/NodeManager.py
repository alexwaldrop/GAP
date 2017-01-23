import time
import logging

from Node import Node

class NodeManager(object):

    def __init__(self, config, platform):

        self.config = config
        self.platform = platform

        self.requires   = dict()
        self.nodes      = dict()
        self.modules    = dict()

        self.generate_graph()

    def generate_graph(self):

        for tool_id in self.config["tools"]:

            tool_data = self.config["tools"][tool_id]

            if not isinstance(tool_data, dict):
                continue

            self.modules[tool_id] = tool_data["module"]
            self.nodes[tool_id] = Node(self.config, self.platform, self.config["sample"], self.modules[tool_id])

            if tool_data["input_from"] != "None":
                self.requires[tool_id] = tool_data["input_from"]
            else:
                self.requires[tool_id] = None

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
                if self.nodes[tool_id].complete:
                    self.nodes[tool_id].finalize()
                    logging.info("Module '%s' has finished!" % self.modules[tool_id])
                    completed.append(tool_id)
                    continue

                done = False

                # Check if tool is still running
                if self.nodes[tool_id].is_alive():
                    continue

                # Launch the tool, if it's required input is done
                req_tool_id = self.requires[tool_id]

                # No requirement
                if req_tool_id is None:
                    self.nodes[tool_id].start()

                # Requirement has finished
                elif self.nodes[req_tool_id].complete:
                    self.nodes[tool_id].start()

                # Requirement has not finished
                else:
                    continue

            # Sleeping for 5 seconds before checking again
            time.sleep(5)