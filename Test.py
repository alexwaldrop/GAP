import logging
from System.Graph import Graph
from System.Datastore import ResourceKit, SampleSet
from System.Validators.GraphValidator import GraphValidator
from System.Platform import StorageHelper, DockerHelper
from System.Platform.Google import GoogleCloudHelper
from GAP import configure_import_paths, configure_logging
import time
import importlib

configure_import_paths()
configure_logging(3)


ss_file = "/home/alex/Desktop/projects/gap/test/test_runs/qc_report_merge/ss.json"
ss = SampleSet(ss_file)
print ss.get_paths()
print ss.get_data(data_type="qc_report", samples="Jijoye_early2")
print ss.get_data(data_type="qc_report", samples="Jijoye_early1")

exit(0)



######################### Test GCH ls command

print GoogleCloudHelper.ls("gs://derp_test/new_gap_test_1/**dummy.txt")
print GoogleCloudHelper.ls("gs://derp_test/*gasdf")

exit(0)

######################### Test GAPfile updating with modules
from System.Platform.Google import GooglePlatform
from System.Datastore import Datastore

# Test cycle checking algorithm
graph_file = "/home/alex/Desktop/projects/gap/test/test_runs/simple_success/graph.config"
graph = Graph(graph_file)

#print graph
rk_file = "/home/alex/Desktop/projects/gap/test/test_runs/simple_success/rk.config"
rk = ResourceKit(rk_file)

ss_file = "/home/alex/Desktop/projects/gap/test/test_runs/simple_success/ss.json"
ss = SampleSet(ss_file)

plat_file = "/home/alex/Desktop/projects/gap/test/test_runs/simple_success/plat.config"
plat = GooglePlatform("test_plat", plat_file, "gs://derp_test/dis_a_test_3/")

dstore = Datastore(graph, rk, ss, plat)


# Test graph splitting
t1 = graph.tasks["tool1_FASTQC"].module
dstore.set_task_input_args("tool1_FASTQC")

print t1.get_arguments()
print t1.get_argument("java")

input_files = dstore.get_task_input_files("tool1_FASTQC")
print input_files

for input_file in input_files:
    print "Before: %s" % input_file.get_path()
    input_file.update_path(new_dir="/data/tool1_FASTQC/")
    print "After: %s" % input_file.get_path()

print t1.get_argument("java")

######################### Test GoogleCloud helper

nr_cpus, mem, instance_type = GoogleCloudHelper.get_optimal_instance_type(1,1,"us-east1-b")
print nr_cpus, mem, instance_type
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b")
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b", is_preemptible=True)
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b", is_preemptible=True, nr_local_ssd=5)
print

nr_cpus, mem, instance_type = GoogleCloudHelper.get_optimal_instance_type(2,12,"us-east1-b")
print nr_cpus, mem, instance_type
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b")
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b", is_preemptible=True)
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b", is_preemptible=True, nr_local_ssd=5)
print

nr_cpus, mem, instance_type = GoogleCloudHelper.get_optimal_instance_type(48,205,"us-east1-b")
print nr_cpus, mem, instance_type
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b")
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b", is_preemptible=True)
print GoogleCloudHelper.get_instance_price(nr_cpus, mem, 500, instance_type, "us-east1-b", is_preemptible=True, nr_local_ssd=5)

exit(0)
######################### Test GAPReport

from System import GAPReport
gr1 = GAPReport(pipeline_id="Dis_a_test")
print gr1
print
print

gr1.set_total_runtime(24.3)
print gr1

gr1.set_fail(err_msg="YOU FUCKED UP")
print gr1

import time
t = (2018, 6, 5, 2, 53, 38, 1, 48, 0)
print time.mktime(t)
print time.time()
gr1.set_start_time(time.mktime(t))

time.sleep(5)
gr1.register_task("task1", time.time(), 3.4, 154.4)
time.sleep(5)
gr1.register_task("task2.split1", time.time(), 1, 2, task_data={"parent_task" : "task2"})
print gr1

gr1.register_output_file("task1", "bam", "/path/to/bam", size=123)
gr1.register_output_file("task1", "bam", "/path/to/bam", size=12)
gr1.register_output_file("task2", "fastq", "/path/to/fastq", size=1235)

print gr1


exit(0)
######################### Test sample validator

from System.Validators import SampleValidator

ss_file = "/home/alex/Desktop/projects/gap/test/ss.bad.json"
ss = SampleSet(ss_file)

sv = SampleValidator(ss)

sv.validate()

exit(0)
######################### InputValidator Tests

from System.Validators import InputValidator

rk_good = "/home/alex/Desktop/projects/gap/test/rk.good.config"
rk_bad  = "/home/alex/Desktop/projects/gap/test/rk.config"
rk = ResourceKit(rk_good)

ss_file = "/home/alex/Desktop/projects/gap/test/ss.json"
ss = SampleSet(ss_file)

proc = GoogleStandardProcessor("test-proc-4", 4, 12, 75, zone="us-east1-c",
                               service_acct="gap-412@davelab-gcloud.iam.gserviceaccount.com",
                               boot_disk_size=75, disk_image="davelab-image-docker")

sh = StorageHelper(proc)
dh = DockerHelper(proc)
iv = InputValidator(rk, ss, sh, dh)

try:
    proc.create()
    proc.__configure_SSH()
    print "We validatin'"
    print iv.validate()
    print "We done validatin'"

finally:
    proc.destroy(wait=False)

exit(0)

######################### Graph tests

# Test cycle checking algorithm
graph_file = "/home/alex/Desktop/projects/gap/test/graph_cycle.config"
graph_file = "/home/alex/Desktop/projects/gap/test/graph_bqsr_test.config"
graph = Graph(graph_file)

#print graph
rk_file = "/home/alex/Desktop/projects/gap/test/rk.config"
rk = ResourceKit(rk_file)

ss_file = "/home/alex/Desktop/projects/gap/test/ss.json"
ss = SampleSet(ss_file)

gv = GraphValidator(graph, rk, ss)
gv.validate()

# Test graph splitting

print ss.get_data(data_type="paired_end")
print ss.get_data(data_type="R1", samples="Jijoye_early2")

print ss.get_paths(samples="Jijoye_early2")

t1 = graph.tasks["split_fastq"].module
t1.set_argument("max_nr_cpus", graph.tasks["split_fastq"].get_graph_config_args()["max_nr_cpus"])
t1.set_argument("nr_reads", graph.tasks["split_fastq"].get_graph_config_args()["nr_reads"])
t1.set_argument("read_len", graph.tasks["split_fastq"].get_graph_config_args()["read_len"])
t1.set_argument("R1", ss.get_data(data_type="R1", samples="Jijoye_early2"))
print graph.tasks["split_fastq"].module.get_command()

t2 = graph.tasks["SPLIT_SAMPLES"].module
t2.set_argument("sample_name", ss.get_data("sample_name"))
t2.get_command()

t3 = graph.tasks["tool11_split_chr"].module
t3.set_argument("chrom_list", graph.tasks["tool11_split_chr"].get_graph_config_args()["chrom_list"])
t3.set_argument("nr_splits", int(graph.tasks["tool11_split_chr"].get_graph_config_args()["nr_splits"]))
t3.get_command()

graph.split_graph(splitter_task_id="tool11_split_chr")
graph.split_graph(splitter_task_id="SPLIT_SAMPLES")
graph.split_graph(splitter_task_id="split_fastq.Jijoye_early1")
graph.split_graph(splitter_task_id="split_fastq.Jijoye_early2")
print graph

gv = GraphValidator(graph, rk, ss)
gv.validate()

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


