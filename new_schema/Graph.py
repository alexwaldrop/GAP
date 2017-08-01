from ConfigParser import ConfigParser
from Node import Node

class Graph(object):

    def __init__(self, pipeline_config_file):

        # Parse and validate pipeline config
        pipeline_config_spec    = "../resources/config_schemas/graph.validate"
        config_parser           = ConfigParser(pipeline_config_file, pipeline_config_spec)
        self.config             = config_parser.get_config()

        # Generate graph
        self.nodes, self.graph = self.__generate_graph()

    def __generate_graph(self):

        nodes  = {}
        graph  = {}

        for node_id in self.config:

            node_data = self.config[node_id]

            graph[node_id] = node_data.pop("input_from")
            nodes[node_id] = Node(node_id, **node_data)

        return nodes, graph

    def get_graph(self):
        return self.graph

    def get_nodes(self):
        return self.nodes
