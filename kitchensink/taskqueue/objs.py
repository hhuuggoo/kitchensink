"""module for rq subclasses
"""
import time
import random
import signal
import os
import sys
import traceback

from rq import Queue, Worker
from rq.job import Job, UNEVALUATED, Status, NoSuchJobError, UnpickleError
import rq.job
from rq.worker import StopRequested
from rq.utils import utcnow
from rq.compat import total_ordering, string_types, as_text
import dill

from ..serialization import serializer, deserializer
from ..utils import setup_loghandlers
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

def nonblock_popall(connection, keys):
    outputs = []
    for k in keys:
        msgs = connection.lrange(k, 0, -1)
        if msgs:
            connection.ltrim(k, len(msgs), -1)
            for m in msgs:
                outputs.append((k, deserializer('json')(m)))
    return outputs

def pop(connection, keys, timeout=5.0):
    logger.info("pop %s", timeout)
    msg = connection.blpop(keys, timeout=timeout)
    if msg is None:
        return None
    k, msg = msg
    msg = deserializer('json')(msg)
    return k, msg

def _grab_all_messages(connection, keys):
    return nonblock_popall(connection, keys)

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

    def set_status(self, status, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection
        self._status = status
        connection.hset(self.key, 'status', self._status)

    def refresh(self):
        st = time.time()
        super(KitchenSinkJob, self).refresh()
        ed = time.time()
        logger.info("REFRESH %s", ed-st)
    def save(self, *args, **kwargs):
        st = time.time()
        super(KitchenSinkJob, self).save(*args, **kwargs)
        ed = time.time()
        logger.info("SAVE %s", ed-st)
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
        connection = pipeline if pipeline is not None else self.connection
        msg = serializer('json')(result)
        connection.rpush(self.intermediate_results_key, msg)

    def cleanup(self, ttl=None, pipeline=None):
        super(KitchenSinkJob, self).cleanup(ttl=ttl, pipeline=pipeline)
        self.connection.expire(self.intermediate_results_key, ttl)

    def push_status(self, status=None, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection
        st = time.time()
        if status is None:
            status = self.get_status()
        self.push_intermediate_results({'type' : 'status',
                                        'status' : status},
                                       pipeline=connection)
        ed = time.time()
        logger.info("push status %s", ed - st)
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
        logger.info("JOB PERFORM")
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
        st = time.time()
        self.connection.sadd(self.redis_queues_keys, self.key)
        ed = time.time()
        logger.info('sadd %s', ed-st)
        if self._async:
            st = time.time()
            self.push_job_id(job.id)
            ed = time.time()
            logger.info('push %s', ed-st)
        else:
            job.perform()
            job.save()
        return job

    @classmethod
    def dequeue_any(cls, queues, timeout, connection=None):
        while True:
            #exits on timeout error
            result = cls._dequeue_any(queues, timeout, connection=connection)
            if result is not None:
                return result

    @classmethod
    def _dequeue_any(cls, queues, timeout, connection=None):
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
        logger.info("DEQUEUED")
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
            return None
        queue = cls.from_queue_key(queue_key, connection=connection)
        try:
            job = cls.job_class.fetch(job_id, connection=connection)
        except NoSuchJobError:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking the same function recursively
            return None
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
        setup_loghandlers()
        super(KitchenSinkWorker, self).work(burst=burst)

    def heartbeat(self, timeout=0, pipeline=None):
        """Specifies a new worker timeout, typically by extending the
        expiration time of the worker, effectively making this a "heartbeat"
        to not expire the worker until the timeout passes.

        The next heartbeat should come before this time, or the worker will
        die (at least from the monitoring dashboards).

        The effective timeout can never be shorter than default_worker_ttl,
        only larger.
        """
        connection = pipeline if pipeline is not None else self.connection
        timeout = max(timeout, self.default_worker_ttl)
        connection.expire(self.key, timeout)
        self.log.debug('Sent heartbeat to prevent worker timeout. '
                       'Next one should arrive within {0} seconds.'.format(timeout))

    def perform_job(self, job):
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.
        """
        self.log.info("**************PERFORM")
        with self.connection._pipeline() as pipeline:
            self.heartbeat((job.timeout or 180) + 60, pipeline=pipeline)
            self.log.info("heartbeat")
            self.set_state('busy', pipeline=pipeline)
            self.log.info("set state")
            self.set_current_job_id(job.id, pipeline=pipeline)
            self.log.info("set job id")
            job.set_status(Status.STARTED, pipeline=pipeline)
            job.push_status(Status.STARTED, pipeline=pipeline)
            self.log.info("execute")
            pipeline.execute()
        self.log.info("before pipeline")
        with self.connection._pipeline() as pipeline:
            try:
                self.log.info("with pipeline")
                with self.death_penalty_class(job.timeout or self.queue_class.DEFAULT_TIMEOUT):
                    rv = job.perform()
                # Pickle the result in the same try-except block since we need to
                # use the same exc handling when pickling fails
                job._result = rv
                job._status = Status.FINISHED
                self.set_current_job_id(None, pipeline=pipeline)
                result_ttl = job.get_ttl(self.default_result_ttl)
                if result_ttl != 0:
                    job.save(pipeline=pipeline)
                job.cleanup(result_ttl, pipeline=pipeline)
                job.push_status(status=Status.FINISHED, pipeline=pipeline)
                pipeline.execute()

            except Exception:
                # Use the public setter here, to immediately update Redis
                self.handle_exception(job, *sys.exc_info())
                job.set_status(Status.FAILED)
                job.push_status()
                return False

        if rv is None:
            self.log.info('Job OK')
        else:
            self.log.info('Job OK, result = %s', rv)

        if result_ttl == 0:
            self.log.info('Result discarded immediately.')
        elif result_ttl > 0:
            self.log.info('Result is kept for %d seconds.' % result_ttl)
        else:
            self.log.warning('Result will never expire, clean up result key manually.')

        return True


def current_job_id():
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.
    """
    job_id = rq.job._job_stack.top
    if job_id is None:
        return None
    else:
        return job_id
