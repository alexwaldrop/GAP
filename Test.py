import logging
from System.Graph import Graph
from System.Datastore import ResourceKit, SampleSet
from System.Validators.GraphValidator2 import GraphValidator
from Main import configure_import_paths
import time
import importlib

configure_import_paths()
######################### Graph tests

# Test cycle checking algorithm
graph_file = "/home/alex/Desktop/projects/gap/test/graph_cycle.config"
graph_file = "/home/alex/Desktop/projects/gap/test/graph.config"
graph = Graph(graph_file)

#print graph
rk_file = "/home/alex/Desktop/projects/gap/test/rk.config"
rk = ResourceKit(rk_file)

ss_file = "/home/alex/Desktop/projects/gap/test/ss.json"
ss = SampleSet(ss_file)

gv = GraphValidator(graph, rk, ss)
gv.validate()

# Test graph splitting
t1 = graph.tasks["split_fastq"].module
t1.set_argument("max_nr_cpus", graph.tasks["split_fastq"].get_graph_config_args()["max_nr_cpus"])
t1.set_argument("nr_reads", graph.tasks["split_fastq"].get_graph_config_args()["nr_reads"])
t1.set_argument("read_len", graph.tasks["split_fastq"].get_graph_config_args()["read_len"])
t1.set_argument("R1", ss.get_data(data_type="R1")["R1"])
print graph.tasks["split_fastq"].module.get_command()

graph.split_graph(splitter_task_id="split_fastq")
print graph

# Test graph validator

# Read RK, SS, Platform


exit(0)

######################### Workspace tests
from System.Datastore.Datastore import TaskWorkspace

wrkspace = TaskWorkspace("/home/alex/wrk", "gs://davelab_tmp/123", "gs://davelab_final/123")
print wrkspace.debug_string()

exit(0)


######################### Module tests
def load_module(module_name, module_id):
    # Try importing the module
    module = importlib.import_module(module_name)
    # Get the class
    _class = module.__dict__[module_name]
    return _class(module_id)


sam_index = load_module("SamtoolsFlagstat", "samflagstat")
sam_index.set_argument("samtools", "/home/samtools")
sam_index.set_argument("bam", "/home/test.bam")
sam_index.set_argument("bam_idx", "/home/test.bam.bai")

print sam_index.get_output_values()
print sam_index.get_input_values()

print sam_index.generate_unique_file_name()
print sam_index.generate_unique_file_name(extension=".txt.fuck.txt")
print sam_index.set_output_dir("/home/new/output")
print sam_index.generate_unique_file_name(extension=".txt.fuck.txt")
print sam_index.get_command()

fastq_splitter = load_module("FastqSplitter", "splitdatfastq")
fastq_splitter.set_output_dir("/home/dis/a/output")

fastq_splitter.set_argument("max_nr_cpus", 32)
fastq_splitter.set_argument("R1", "/data/r1.fastq")
#fastq_splitter.set_argument("R2", "/data/r2.fastq")
fastq_splitter.set_argument("nr_reads", 10000000)
fastq_splitter.set_argument("read_len", 200)
print fastq_splitter.get_command()

print fastq_splitter.get_output_values()
print len(fastq_splitter.get_output_values())
print fastq_splitter.get_output()
for output in fastq_splitter.get_output_values():
    print output

exit(0)
########################################
graph_file = "/home/alex/Desktop/projects/sad_monkey/gap/RNAseq/upstream/mmul8_RNAseq_PE_upstream_graph.config"
rk_file = "/home/alex/Desktop/projects/gap/rk.config"

from System.Datastore.ResourceKit import ResourceKit

rk = ResourceKit(rk_file)

print rk.get_resources("samtools")

for docker_id, docker in rk.get_docker_images().iteritems():
    print docker.get_image_name()
    for file_type, file_data in docker.get_resources().iteritems():
        print "File type: %s" % file_type
        for file_id, gap_file in file_data.iteritems():
            print gap_file
            print gap_file.debug_string()
            print gap_file.is_flagged("docker")
exit(0)
print "Samtools: %s" % rk.has_resource_type("samtools")
print "Derp: %s" % rk.has_resource_type("derp")
print "hamtools docker: %s" % rk.has_docker_image("hamtools")
print "hamtools has samtools: %s" % rk.get_docker_images("hamtools").get_resources("samtools")
exit(0)


######################### Graph Tests

#src_path = "/home/alex/test.txt"
#dest_path = "gs://home/alex/dest.txt"

from System.Platform import StorageHelper

from System.Platform.Google import GoogleStandardProcessor

proc = GoogleStandardProcessor("test-proc", 1, 1, zone="us-east1-b",
                               service_acct="gap-412@davelab-gcloud.iam.gserviceaccount.com",
                               boot_disk_size=75, disk_image="davelab-image-latest")

proc.set_log_dir("/home/gap/log/")
proc.create()

print "We READY TO RUN!"

try:
    sh = StorageHelper(proc)
    sh.mkdir("/home/alex_waldrop_jr/test/", wait=True)
    sh.mkdir("/home/gap/log/", wait=True)
    sh.mkdir("gs://derp_test/mkdir_test_mofo_2/", wait=True)
    proc.run("perms_gap", "sudo chmod -R 777 /home/gap/")
    proc.run("perms_awal", "sudo chmod -R 777 /home/alex_waldrop_jr/")
    proc.wait()
    print "local exists: %s" % sh.path_exists("/home/alex_waldrop_jr/test/")
    print "local exists: %s" % sh.path_exists("/home/gap/log/")
    print "cloud exists: %s" % sh.path_exists("gs://derp_test/mkdir_test_mofo_2/")
    print "bad exists: %s" % sh.path_exists("/home/aasdasdfk")
    sh.mv("gs://derp_test/dummy.txt", "/home/alex_waldrop_jr/test/", log=False, wait=True)
    sh.mv("/home/alex_waldrop_jr/test/dummy.txt", "/home/alex_waldrop_jr/test/whoops_i_win.txt", log=False, wait=True)
    sh.mv("/home/alex_waldrop_jr/test/whoops_i_win.txt", "gs://derp_test/mkdir_test_mofo_2/", log=False, wait=True)
    print "good file exists: %s" % sh.path_exists("/home/alex_waldrop_jr/test/dummy.txt")
    print "DNA file size: %s" % sh.get_file_size("gs://davelab_data/ref/hg19/DNA")
    print "Dummy file size: %s" % sh.get_file_size("/home/alex_waldrop_jr/test/whoops_i_win.txt")

finally:
    time.sleep(120)
    proc.destroy()


