from System.Graph import Graph
from Main import configure_import_paths

######################### Graph Tests
configure_import_paths()
graph_file = "/home/alex/Desktop/projects/sad_monkey/gap/RNAseq/upstream/mmul8_RNAseq_PE_upstream_graph.config"


graph = Graph(graph_file)

print graph.get_nodes()
print graph.get_adjacency_list()

print
print

new_node = graph.get_nodes()["tool13_RSEM"].split("BURP")
graph.add_node(new_node)
print graph.get_nodes()
print graph.get_adjacency_list()

print graph.get_nodes()["tool13_RSEM"] is graph.get_nodes()["BURP"]
print
print

graph.add_edge("BURP", "tool6_FLAGSTAT")
print graph.get_nodes()
print graph.get_adjacency_list()

print
print

#graph.remove_node("tool6_FLAGSTAT")
#print graph.get_nodes()
#print graph.get_adjacency_list()

print
print
new_graph = graph.get_subgraph(["BURP", "tool7_STAR", "tool18_UTR_OVERLAP"])
print new_graph.get_nodes()
print new_graph.get_adjacency_list()


print
print
print graph.get_dependents("tool2_TRIMMOMATIC")
print graph.get_dependents("tool6_FLAGSTAT")

######################### Graph Tests