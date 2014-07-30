"""module for rq subclasses
"""
import time
import random
import signal
import os
import sys

from rq import Queue, Worker
from rq.job import Job, UNEVALUATED, Status, NoSuchJobError
import rq.job
from rq.worker import StopRequested
from rq.logutils import setup_loghandlers
from rq.utils import utcnow
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
    logger.info("nonblock pop")
    for k in keys:
        msg = connection.lpop(k)
        if msg:
            msg = deserializer('json')(msg)
            return k, msg
    return None

def pop(connection, keys, timeout=5.0):
    logger.info("pop")
    msg = connection.blpop(keys, timeout=timeout)
    if msg is None:
        return None
    k, msg = msg
    msg = deserializer('json')(msg)
    return k, msg

def _grab_all_messages(connection, keys):
    logger.info("grab all messages")
    messages = []
    while True:
        msg = nonblock_pop(connection, keys, timeout=0.0)
        if msg is None:
            break
        k, msg = msg
        messages.append((k, msg))
    return messages

def _block_and_grab_all_messages(connection, keys, timeout=5.0):
    logger.info("block and grab all messages")
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

    def push_intermediate_results(self, result, pipeline=None):
        if pipeline:
            connection = pipeline
        else:
            connection = self.connection
        msg = serializer('json')(result)
        connection.rpush(self.intermediate_results_key, msg)

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
                                             timeout=timeout)
        messages = [x[1] for x in messages]
        return messages

    # Job execution
    def perform(self):  # noqa
        """Invokes the job function with the job arguments."""
        rq.job._job_stack.push(self.id)
        try:
            self._result = self.func(*self.args, **self.kwargs)
            self.ended_at = utcnow()
        finally:
            assert self.id == rq.job._job_stack.pop()
        return self._result

class KitchenSinkRedisQueue(Queue):
    DEFAULT_TIMEOUT = 86400
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

    def perform_job(self, job):
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.
        """

        self.set_state('busy')
        self.set_current_job_id(job.id)
        self.heartbeat((job.timeout or 180) + 60)

        self.procline('Processing %s from %s since %s' % (
            job.func_name,
            job.origin, time.time()))
        job.set_status(Status.STARTED)
        job.push_status()
        with self.connection._pipeline() as pipeline:
            try:

                with self.death_penalty_class(job.timeout or self.queue_class.DEFAULT_TIMEOUT):
                    rv = job.perform()
                # Pickle the result in the same try-except block since we need to
                # use the same exc handling when pickling fails
                job._result = rv
                self.set_current_job_id(None, pipeline=pipeline)
                result_ttl = job.get_ttl(self.default_result_ttl)
                if result_ttl != 0:
                    job.save(pipeline=pipeline)
                job.cleanup(result_ttl, pipeline=pipeline)
                pipeline.execute()

            except Exception:
                # Use the public setter here, to immediately update Redis
                self.handle_exception(job, *sys.exc_info())
                job.set_status(Status.FAILED)
                job.push_status()
                return False
        job.set_status(Status.FINISHED)
        job.push_status()
        if rv is None:
            self.log.info('Job OK')
        else:
            self.log.info('Job OK, result = %s' % rv)

        if result_ttl == 0:
            self.log.info('Result discarded immediately.')
        elif result_ttl > 0:
            self.log.info('Result is kept for %d seconds.' % result_ttl)
        else:
            self.log.warning('Result will never expire, clean up result key manually.')

        return True

def get_current_job(connection=None):
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.
    """
    job_id = rq.job._job_stack.top
    if job_id is None:
        return None
    return KitchenSinkJob.fetch(job_id, connection=connection)
