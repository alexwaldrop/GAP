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

    def run(self):

        while not self.task_graph.is_complete():

            # Check all tasks to see if they need anything updated
            unfinished_tasks = self.task_graph.get_unfinished_tasks()
            for task in unfinished_tasks:

                # Task id
                task_id = task.get_ID()

                # Check if task worker has been created for task
                task_worker = None if task_id not in self.task_workers else self.task_workers[task_id]

                # Finalize completed tasks
                if task_worker is not None and task_worker.get_status() is TaskWorker.COMPLETE:
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

                # At least one of node's requirements is not complete, so this node cannot be processed yet
                if not self.task_graph.parents_complete(task_id):
                    continue

                logging.info("Launching task: '%s'" % task_id)
                # Task dependencies are met. Initialize task graph.
                self.task_workers[task_id] = TaskWorker(task, self.datastore, self.platform)
                self.task_workers[task_id].start()

            # Sleeping for 5 seconds before checking again
            time.sleep(5)

    def __launch(self, task_worker):
        # Specify that task worker can now run task
        task_worker.set_status(TaskWorker.RUNNING)
        # Adjust current resource usage levels
        self.cpus += task_worker.task.get_cpus()
        self.mem += task_worker.task.get_mem()
        self.disk_space += task_worker.task.get_disk_space()
