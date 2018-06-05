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

    def get_task_workers(self):
        return self.task_workers

    def run(self):
        try:
            self.__run_tasks()
        finally:
            self.__finalize()

    def __run_tasks(self):
        # Execute tasks until are are completed or until error encountered
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
                    logging.info("Task '%s' has finished!" % task_id)
                    self.__finalize_task_worker(task_worker)
                    continue

                # Case: At least one of node's requirements is not complete, so this node cannot be processed yet
                if not self.task_graph.parents_complete(task_id):
                    continue

                # Case: Task with no task worker has dependencies met. Start running new task!
                logging.info("Launching task: '%s'" % task_id)
                self.task_workers[task_id] = TaskWorker(task, self.datastore, self.platform)
                self.task_workers[task_id].start()

            # Sleeping for 5 seconds before checking again
            time.sleep(5)

    def __finalize_task_worker(self, task_worker):

        # Get task being executed by worker
        task = task_worker.get_task()

        # Add to list of finalized task workers
        task_worker.set_status(TaskWorker.FINALIZED)

        # Checks for and raises any runtime errors that occurred while running task
        task_worker.finalize()

        # Split subgraph if task is a splitter
        if task.get_type() == "Splitter":
            self.task_graph.split_graph(task.get_ID())

        # Set task to complete
        task.set_complete(True)

    def __finalize(self):

        # Prevent any new processors from being created on platform
        self.platform.lock()

        # Cancel any still-running jobs
        self.__cancel_unfinished_tasks()

        # Wait for all jobs to finish
        done = False
        while not done:
            # Wait for all task workers to finish up cancelling
            done = True
            for task_id, task_worker in self.task_workers:
                if not task_worker.get_status() is TaskWorker.FINALIZED:
                    # Indicate that not all tasks have been finalized
                    done = False
                elif task_worker.get_status() is TaskWorker.COMPLETE:
                    # Finalize task if it hasn't already been finalized
                    try:
                        self.__finalize_task_worker(task_worker)

                    except BaseException, e:
                        # Log error but don't raise exception as we want to finish finalizing all task workers
                        logging.error("Task '%s' failed!" % task_id)
                        if e.message != "":
                            logging.error("Received the following message:\n%s" % e.message)

            # Wait for a bit before checking again
            time.sleep(5)

    def __cancel_unfinished_tasks(self):
        # Cancel any still-running jobs
        for task_id, task_worker in self.task_workers:
            if not task_worker.get_status() in [TaskWorker.COMPLETE, TaskWorker.FINALIZING, TaskWorker.FINALIZED]:
                # Cancel pipeline if it isn't finalizing or already cancelled
                task_worker.cancel()


