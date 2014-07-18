"""module for rq subclasses
"""
from rq import Queue, Worker
from rq.job import Job, UNEVALUATED, Status
import rq.job
from rq.logutils import setup_loghandlers
import dill

from ..serialization import serializer, deserializer
import logging
logger = logging.getLogger(__name__)

class KitchenSinkJob(Job):
    @property
    def intermediate_results_key(self):
        return self.key + ":" + "intermediate_results"

    def push_intermediate_results(self, result):
        msg = serializer('json')(result)
        self.connection.rpush(self.intermediate_results_key, msg)

    def cleanup(self, ttl=None, pipeline=None):
        super(KitchenSinkJob, self).cleanup(ttl=ttl, pipeline=pipeline)
        self.connection.expire(self.intermediate_results_key, ttl)

    def push_status(self):
        status = self.get_status()
        self.push_intermediate_results({'type' : 'status',
                                        'status' : status})
    def push_stdout(self, output):
        self.push_intermediate_results({'type' : 'stdout',
                                        'msg' : output})
    def _grab_all_messages(self):
        messages = []
        while True:
            msg = self.connection.lpop(self.intermediate_results_key)
            if msg is None:
                break
            msg = deserializer('json')(msg)
            messages.append(msg)
        return messages

    def _block_and_grab_all_messages(self, timeout=5.0):
        result = self.connection.blpop(self.intermediate_results_key,
                                       timeout=timeout)
        if result:
            _, msg =  result
        else:
            msg = None
        if msg:
            logger.info("msg:%s:" % str(msg))
            msg = deserializer('json')(msg)
            messages = self._grab_all_messages()
            return [msg] + messages
        else:
            return []

    def pull_intermediate_results(self, timeout=5):
        """pull all messages off the queue.  We pull objects
        in a non blocking manner first.  If we something, we return that
        something. if we get nothing, then we do a blocking pop
        for timeout
        """
        messages = self._grab_all_messages()
        if messages:
            return messages
        else:
            return self._block_and_grab_all_messages(timeout=timeout)








class KitchenSinkRedisQueue(Queue):
    job_class = KitchenSinkJob

class KitchenSinkWorker(Worker):
    job_class = KitchenSinkJob
    queue_class = KitchenSinkRedisQueue
    def work(self, burst=False):
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """
        setup_loghandlers()
        self._install_signal_handlers()

        did_perform_work = False
        self.register_birth()
        self.set_state('starting')
        try:
            while True:
                if self.stopped:
                    self.log.info('Stopping on request.')
                    break

                timeout = None if burst else max(1, self.default_worker_ttl - 60)
                try:
                    result = self.dequeue_job_and_maintain_ttl(timeout)
                    if result is None:
                        break
                except StopRequested:
                    break

                job, queue = result
                job.push_status()
                self.execute_job(job)
                job.push_status()
                self.heartbeat()

                if job.get_status() == Status.FINISHED:
                    queue.enqueue_dependents(job)

                did_perform_work = True
        finally:
            if not self.is_horse:
                self.register_death()
        return did_perform_work

def get_current_job(connection=None):
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.
    """
    job_id = rq.job._job_stack.top
    if job_id is None:
        return None
    return KitchenSinkJob.fetch(job_id, connection=connection)