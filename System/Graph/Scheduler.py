import logging
import time

from TaskWorker import TaskWorker

class Scheduler(object):

    def __init__(self, task_graph, datastore, platform):

        # Initialize pipeline definition variables
        self.task_graph     = task_graph
        self.datastore      = datastore
        self.platform       = platform

        # Initialize set of task workers
        self.task_workers = {}

        # Scheduler resource limits
        self.max_cpus       = 100
        self.max_mem        = 100
        self.max_disk_space = 100

        # Current scheduler resource usage
        self.cpus           = 0
        self.mem            = 0
        self.disk_space     = 0

    def run(self):

        while not self.task_graph.is_complete:

            # Check all tasks to see if they need anything updated
            for task in self.task_graph.get_tasks():

                # Task id
                task_id = task.get_ID()

                # Skip already completed nodes
                if task.complete:
                    continue

                # Check if task worker has been created for task
                task_worker = None if task_id not in self.task_workers else self.task_workers[task_id]
                if task_worker is not None and task_worker.get_status() is TaskWorker.COMPLETE:
                    # Finalize completed tasks
                    # Log the completion
                    logging.info("Task '%s' has finished!" % task_id)

                    # Throw any runtime errors
                    task_worker.finalize()

                    # Split subgraph if task was a splitter
                    if task.get_type() == "Splitter":
                        self.task_graph.split_graph(task)

                    # Set task to complete
                    task.set_complete(True)
                    continue

                # Launch any pending tasks that have enough resources to run
                if task_worker is not None and task_worker.get_status() is TaskWorker.READY:
                    # Launch pending task if enough resources have cleared up
                    if self.__can_launch(task_worker):
                        self.__launch_worker(task_worker)
                        # TODO: Keep decrement resources from Scheduler resource pool
                    continue

                # At least one of node's requirements is not complete, so this node cannot be processed yet
                if not self.task_graph.parents_complete(task_id):
                    continue

                # Task dependencies are met. Initialize task graph.
                self.task_workers[task_id] = TaskWorker(task, self.datastore, self.platform)
                self.task_workers[task_id].start()

            # Sleeping for 5 seconds before checking again
            time.sleep(5)

    def __can_launch(self, task_worker):
        # Return true if enough resources are available to run job
        new_cpus        = self.cpus + task_worker.get_cpus()
        new_mem         = self.mem + task_worker.get_mem()
        new_disk_space  = self.disk_space = task_worker.get_disk_space()
        return new_cpus < self.max_cpus and new_mem <= self.max_mem and new_disk_space <= self.disk_space

    def __launch_worker(self, task_worker):
        # Specify that task worker can now run task
        task_worker.set_status(TaskWorker.RUNNING)
        # Adjust current resource usage levels
        self.cpus += task_worker.get_cpus()
        self.mem += task_worker.get_mem()
        self.disk_space += task_worker.get_disk_space()
