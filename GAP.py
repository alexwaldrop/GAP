#!/usr/bin/env python2.7

from GAP_system import Config, TaskManager, Task

# Generating the config object
config = Config("GAP.config")

from GAP_modules import FASTQSplitter as Splitter
from GAP_modules import SamtoolsSamToBam as ConverterSamToBam
from GAP_modules import SamtoolsBAMMerge as BAMMerger

if config.cluster.ID == 0:
    from GAP_modules import SLURM as Cluster

if config.aligner.ID == 0:
    from GAP_modules import BwaAligner as Aligner

splitter    = Splitter(config)
converter   = ConverterSamToBam(config)
merger      = BAMMerger(config)
cluster     = Cluster(config)
aligner     = Aligner(config)
task_manager = TaskManager(config, cluster)

splitted = config.cluster.nodes > 1

# Splitting the FASTQ file(s)
if splitted:
    task = Task("split", config, cluster)

    task.type       = "split"
    task.nodes      = 1
    task.mincpus    = 1
    task.command    = splitter.byNrReads(int(1e6), config.paths.R1, config.paths.R2)

    task_manager.addTask(task)

    # Getting the number of splits
    split_count = splitter.split_count

if config.general.goal == "align":
    if splitted:
        for split_id in range(split_count):
            task = Task("align_%d" % split_id, config, cluster)
            task.type       ="align"

            task.nodes      = 1
            task.mincpus    = config.cluster.mincpus
            task.requires   = ["split"]

            aligner.R1      = "%s/fastq_R1_%04d" % (config.general.temp_dir, split_id)
            aligner.R2      = "%s/fastq_R2_%04d" % (config.general.temp_dir, split_id)
            aligner.threads = task.mincpus

            converter.threads = task.mincpus
            if aligner.to_stdout:
                if aligner.output_type == "sam":
                    task.command = "%s | %s > %s/bam_%04d" % (aligner.getCommand(), converter.getCommand(), config.general.temp_dir, split_id)
                else:
                    task.command = "%s > %s/bam_%04d" % (aligner.getCommand(), config.general.temp_dir, split_id)
            else:
                task.command = aligner.getCommand()

            task_manager.addTask(task)
    else:
        task = Task("align", config, cluster)
        task.type     = "align"
        
        task.nodes    = 1
        task.mincpus  = config.cluster.mincpus
        
        aligner.R1      = config.paths.R1
        aligner.R2      = config.paths.R2
        aligner.threads = task.mincpus

        converter.threads = task.mincpus
        if aligner.to_stdout:
            if aligner.output_type == "sam":
                task.command = "%s | %s > %s/out.bam" % (aligner.getCommand(), converter.getCommand(), config.general.output_dir)
            else:
                task.command = "%s > %s/out.bam" % (aligner.getCommand(), config.general.output_dir)
        else:
            task.command = aligner.getCommand()

        task_manager.addTask(task)

if splitted:
    task = Task("merge", config, cluster)
    task.type       = "merge"

    task.nodes      = 1
    task.mincpus    = config.cluster.mincpus
    task.requires   = ["align_%d" % split_id for split_id in range(split_count)]

    merger.nr_splits= split_count
    task.command    = merger.getCommand()

    task_manager.addTask(task)

task_manager.run()
