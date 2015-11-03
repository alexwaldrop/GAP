#!/usr/bin/env python2.7

from GAP_system import Config, TaskManager, Task

# Generating the config object
config = Config("GAP.config")

if config.cluster.ID == 0:
    from GAP_modules import SLURM as Cluster

if config.aligner.ID == 0:
    from GAP_modules import BwaAligner as Aligner

cluster     = Cluster(config)
aligner     = Aligner(config)
task_manager = TaskManager(config, cluster)

if config.general.goal == "align":
    if config.cluster.nodes == 1:
        task = Task("align", config, cluster)
        task.type     = "align"
        
        task.nodes    = 1
        task.mincpus  = config.cluster.mincpus
        
        aligner.threads = task.mincpus
        if aligner.to_stdout:
            task.command = "%s > %s/out.sam" % (aligner.getCommand(), config.general.output_dir)
        else:
            task.command = aligner.getCommand()

        task_manager.addTask(task)

task_manager.run()
