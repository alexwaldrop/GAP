import importlib
import time
import logging

from GAP_interfaces import Main

def initialize_module(module_name):

    d = dict()
    d["module_name"] = module_name
    d["module"] = importlib.import_module("GAP_modules.%s" % d["module_name"])
    d["class_name"] = d["module"].__main_class__
    d["class"] = d["module"].__dict__[d["class_name"]]

    return d

class Node(object):

    def __init__(self, config, platform, sample_data, module_name):

        self.config = config
        self.platform = platform
        self.sample_data = sample_data
        self.module_name = module_name

        # Importing main module
        try:
            self.main = initialize_module(module_name)
            self.main_obj = self.main["class"](self.config, self.sample_data)
        except ImportError:
            logging.error("Module %s cannot be imported!" % module_name)
            exit(1)

        # Importing splitter and merger:
        if self.main_obj.can_split:

            try:
                self.split = initialize_module(self.main_obj.splitter)
                self.split_obj = self.split["class"](self.config, self.sample_data)
            except ImportError:
                logging.error("Module %s cannot be imported!" % self.main_obj.splitter)
                exit(1)

            try:
                self.merge = initialize_module(self.main_obj.merger)
                self.merge_obj = self.merge["class"](self.config, self.sample_data)
            except ImportError:
                logging.error("Module %s cannot be imported!" % self.main_obj.merger)
                exit(1)

        self.split_outputs = None
        self.main_outputs  = None
        self.merge_outputs = None

    def run_split(self):

        # Creating job names
        split_job_name  = "%s_split" % self.module_name
        main_job_name   = lambda split_id: "%s_%d" % (self.module_name, split_id)
        merge_job_name  = "%s_merge" % self.module_name

        # Running the splitter
        cmd = self.split_obj.get_command( nr_splits=self.config["general"]["nr_splits"] )
        self.platform.instances["main-server"].run_command(split_job_name, cmd)
        self.platform.instances["main-server"].wait_all()

        self.split_outputs = self.split_obj.get_output()

        # Creating the split servers
        self.platform.create_split_servers(self.config["general"]["nr_splits"], nr_cpus=self.config["instance"]["nr_cpus"],
                                           is_preemptible=True, nr_local_ssd=0)

        self.main_outputs = list()

        # Calling the process on each split
        for split_id, paths in enumerate(self.split_outputs):
            cmd = self.main_obj.get_command( split_id=split_id, **paths )
            self.main_outputs.append( self.main_obj.get_output() )

            self.platform.instances["split%d-server" % split_id].run_command(main_job_name(split_id), cmd)

        # Waiting for all the split aligning processes to finish
        while True:

            still_running = False

            for instance_name, instance_obj in self.platform.instances.iteritems():
                if instance_name.startswith("split"):

                    # Generating process name
                    split_id = int(instance_name.split("-")[0].split("split")[-1])
                    proc_name = main_job_name(split_id)

                    # Check if process is done
                    if instance_obj.poll_process(proc_name):

                        # Skipping instances that are resetting
                        if instance_obj.is_preemptible and not instance_obj.available_event.is_set():
                            continue

                        # Skipping process that has already been registered
                        if instance_obj.processes[proc_name].complete:
                            continue

                        # Skipping process that has previously failed and not marked complete
                        if instance_obj.processes[proc_name].has_failed():
                            still_running = True
                            continue

                        # Checking the complete process
                        instance_obj.wait_process(proc_name)

                        # Destroy the split
                        instance_obj.destroy()

                    else:
                        still_running = True

            if still_running:
                time.sleep(5)
            else:
                break

        # Running the merger
        cmd = self.merge_obj.get_command( nr_splits= self.config["general"]["nr_splits"],
                                          inputs=self.main_outputs )
        self.platform.instances["main-server"].run_command(merge_job_name, cmd)

        # Waiting for all the split servers to DIE!!!
        for instance_name, instance_obj in self.platform.instances.iteritems():
            if instance_name.startswith("split"):
                instance_obj.wait_all()

        # Waiting for the merging to finish
        self.platform.instances["main-server"].wait_all()

        self.merge_outputs = self.merge_obj.get_output()

        # Marking for output
        if "outputs" not in self.sample_data:
            self.sample_data["outputs"] = [ self.merge_outputs ]
        else:
            self.sample_data["outputs"].append( self.merge_outputs )

    def run_normal(self):

        cmd = self.main_obj.get_command()

        self.platform.instances["main-server"].run_command(self.module_name, cmd)
        self.platform.instances["main-server"].wait_all()

        # Marking for output
        if "outputs" not in self.sample_data:
            self.sample_data["outputs"] = [self.main_obj.get_output()]
        else:
            self.sample_data["outputs"].append(self.main_obj.get_output())

    def run(self):

        if self.main_obj.can_split and self.config["general"]["split"]:
            self.run_split()
        else:
            self.run_normal()