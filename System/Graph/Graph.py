from Config import ConfigParser
from System.Graph import Node

class Graph(object):

    def __init__(self, pipeline_config_file):

        # Parse and validate pipeline config
        pipeline_config_spec    = "Config/Schema/Graph.validate"
        config_parser           = ConfigParser(pipeline_config_file, pipeline_config_spec)
        self.config             = config_parser.get_config()

        # Generate graph
        self.nodes, self.adj_list = self.__generate_graph()

    def __generate_graph(self):

        nodes  = {}
        adj_list  = {}

        for node_id in self.config:

            node_data = self.config[node_id]

            adj_list[node_id] = node_data.pop("input_from")
            nodes[node_id] = Node(node_id, **node_data)

        return nodes, adj_list

    def get_adjacency_list(self):
        return self.adj_list

    def get_nodes(self):
        return self.nodes
