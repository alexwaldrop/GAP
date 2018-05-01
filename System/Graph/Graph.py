import logging
import copy

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
        self.__check_adjacency_list()

    def add_node(self, node):
        # Connect new node to existing graph
        if node.get_ID() in self.nodes:
            logging.error("Graph Error: Attempt to add duplicate node to graph: %s" % node.get_ID())
            raise RuntimeError("Cannot add node with duplicate ID to graph!")

        # Add new new to nodelist
        self.nodes[node.get_ID()] = node
        self.adj_list[node.get_ID()] = []

    def remove_node(self, node_id):
        # Remove node and all edges from Graph
        if node_id not in self.nodes:
            logging.error("Attempt to remove non-existant node from Graph: %s" % node_id)
            raise RuntimeError("Graph Error: Attempt to remove non-existant node from graph!")

        # Remove node from vertice list
        self.nodes.pop(node_id)
        self.adj_list.pop(node_id)

        # Remove all references to node in adjacency list
        for adj_list in self.adj_list.itervalues():
            if node_id in adj_list:
                adj_list.remove(node_id)

    def add_edge(self, dep_node_id, ind_node_id):
        # Adds dependency where dep_nod_id must wait until ind_node_id is finished
        if dep_node_id not in self.nodes:
            logging.error("Unable to add edge to graph! Unknown node: %s!" % dep_node_id)
            raise RuntimeError("Attempt to add edge between non-existant nodes!")
        if ind_node_id not in self.nodes:
            logging.error("Unable to add edge to graph! Unknown node: %s!" % ind_node_id)
            raise RuntimeError("Attempt to add edge between non-existant nodes!")

        # Add dependency
        self.adj_list[dep_node_id].append(ind_node_id)

    def get_dependents(self, node_id):
        if node_id not in self.nodes:
            logging.error("Cannot list dependents for non-existant node: %s" % node_id)
            raise RuntimeError("Graph Error: Attempt to get dependents from nonexistant node!")
        dependents = []
        for node, edges in self.adj_list.iteritems():
            if node_id in edges:
                dependents.append(node)
        return dependents

    def get_adjacency_list(self):
        return self.adj_list

    def get_nodes(self):
        return self.nodes

    def get_subgraph(self, node_ids):
        # Get subgraph containing only specific nodes
        new_graph = copy.deepcopy(self)
        for node in self.nodes:
            if node not in node_ids:
                new_graph.remove_node(node)
        return new_graph

    @property
    def is_complete(self):
        for node in self.nodes:
            if not node.is_complete():
                return False
        return True

    def __generate_graph(self):

        nodes  = {}
        adj_list  = {}

        for node_id in self.config:

            node_data = self.config[node_id]

            adj_list[node_id] = node_data.pop("input_from")
            nodes[node_id] = Node(node_id, **node_data)

        return nodes, adj_list

    def __check_adjacency_list(self, runtime=False):
        errors = False
        for node, adj_nodes in self.adj_list.iteritems():
            for adj_node in adj_nodes:
                if adj_node not in self.nodes:
                    if not runtime:
                        logging.error("Incorrect pipeline graph! Node '%s' receives input from an undeclared node: '%s'. "
                                      "Please correct in the pipeline graph config!" % (node, adj_node))
                    else:
                        logging.error("Incorrect pipeline graph! Node '%s' receives input from an undeclared node: '%s'. "
                                      % (node, adj_node))
                    errors = True
        if errors:
            if not runtime:
                raise IOError("Incorrect pipeline graph defined in graph config!")
            else:
                raise RuntimeError("Runtime graph alteration resulted in invalid graph!")
