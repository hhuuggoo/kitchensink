"""module for rq subclasses
"""
import time

from rq import Queue, Worker
from rq.job import Job, UNEVALUATED, Status, NoSuchJobError
import rq.job
from rq.worker import StopRequested
from rq.logutils import setup_loghandlers
from rq.compat import total_ordering, string_types, as_text
import dill

from ..serialization import serializer, deserializer
import logging
logger = logging.getLogger(__name__)

def empty(result):
    """result of pop operation. dict of lists
    """
    for r in result.values():
        if len(r) != 0:
            return True
    return False

def nonblock_pop(connection, keys, timeout=5.0):
    for k in keys:
        msg = connection.lpop(k)
        if msg:
            msg = deserializer('json')(msg)
            return k, msg
    return None

def pop(connection, keys, timeout=5.0):
    if timeout == 0:
        import pdb; pdb.set_trace()
    msg = connection.blpop(keys, timeout=timeout)
    if msg is None:
        return None
    k, msg = msg
    msg = deserializer('json')(msg)
    return k, msg

def _grab_all_messages(connection, keys):
    messages = []
    while True:
        msg = nonblock_pop(connection, keys, timeout=0.0)
        if msg is None:
            break
        k, msg = msg
        messages.append((k, msg))
    return messages

def _block_and_grab_all_messages(connection, keys, timeout=5.0):
    messages = []
    msg = pop(connection, keys, timeout=timeout)
    if msg is None:
        return []
    else:
        messages = _grab_all_messages(connection, keys)
        return [msg] + messages

def pull_intermediate_results(connection, keys, timeout=5):
    """pull all messages off the queue.  We pull objects
    in a non blocking manner first.  If we something, we return that
    something. if we get nothing, then we do a blocking pop
    for timeout
    """
    keys = ["rq:job:"+ x + ":intermediate_results" for x in keys]
    messages = _grab_all_messages(connection, keys)
    #if timeout is 0, then we want instant return, do not do blocking call
    if not messages and timeout != 0:
        messages =  _block_and_grab_all_messages(connection, keys, timeout=timeout)
    messages = [(x[0].split(":")[2], x[1]) for x in messages]
    return messages

class KitchenSinkJob(Job):
    @property
    def intermediate_results_key(self):
        return self.key + ":" + "intermediate_results"

    @classmethod
    def claim_key_for(cls, job_id):
        return cls.key_for(job_id) + ":claim"

    @classmethod
    def claim_for(cls, connection, job_id, queue_name):
        val = connection.setnx(cls.claim_key_for(job_id), queue_name)
        if val == 0:
            logger.info("failed to claim %s for %s", job_id, queue_name)
            return False
        else:
            logger.info("succeeded to claim %s for %s", job_id, queue_name)
            return True

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

    def pull_intermediate_results(self, timeout=5):
        """pull all messages off the queue.  We pull objects
        in a non blocking manner first.  If we something, we return that
        something. if we get nothing, then we do a blocking pop
        for timeout
        """
        messages = pull_intermediate_results(self.connection,
                                             [self.id],
                                             timeout=self.timeout)
        messages = [x[1] for x in messages]
        return messages

class KitchenSinkRedisQueue(Queue):
    job_class = KitchenSinkJob
    def enqueue_job(self, job, set_meta_data=True):
        """Enqueues a job for delayed execution.

        If the `set_meta_data` argument is `True` (default), it will update
        the properties `origin` and `enqueued_at`.

        If Queue is instantiated with async=False, job is executed immediately.
        """
        # Add Queue key set
        self.connection.sadd(self.redis_queues_keys, self.key)
        if self._async:
            self.push_job_id(job.id)
        else:
            job.perform()
            job.save()
        return job

    @classmethod
    def dequeue_any(cls, queues, timeout, connection=None):
        """Class method returning the job_class instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        See the documentation of cls.lpop for the interpretation of timeout.
        """
        queue_keys = [q.key for q in queues]
        result = cls.lpop(queue_keys, timeout, connection=connection)
        if result is None:
            return None
        queue_key, job_id = map(as_text, result)
        if connection is None:
            connection = self.connection
        """We now enqueue jobs on multiple queues, so when we pop them
        we have to try to claim it.  if we can't claim it, discard,
        that means another worker has it
        """
        if not cls.job_class.claim_for(connection, job_id, queue_key):
            return cls.dequeue_any(queues, timeout, connection=connection)
        queue = cls.from_queue_key(queue_key, connection=connection)
        try:
            job = cls.job_class.fetch(job_id, connection=connection)
        except NoSuchJobError:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking the same function recursively
            return cls.dequeue_any(queues, timeout, connection=connection)
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.job_id = job_id
            e.queue = queue
            raise e
        return job, queue

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
