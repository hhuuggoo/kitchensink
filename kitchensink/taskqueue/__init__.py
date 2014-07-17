import copy

from rq.job import Job, Status
from rq import Queue
from rq import Connection

from .objs import KitchenSinkJob, KitchenSinkRedisQueue

class TaskQueue(object):
    def __init__(self, redis_conn):
        self.conn = redis_conn
        self.queues = {}

    def get_queue(self, name):
        if not name in self.queues:
            with Connection(self.conn):
                queue = KitchenSinkRedisQueue(name)
                self.queues[name] = queue
        return self.queues[name]

    def enqueue(self, queue_name, func, args, kwargs, metadata={}):
        queue = self.get_queue(queue_name)
        job = self.get_queue(queue_name).enqueue_call(func, args=args, kwargs=kwargs)
        print type(job)
        for k,v in metadata.items():
            job.meta[k] = v
        job.save()
        return job._id, job.get_status()

    def status(self, job_id, timeout=None):
        """timeout is None - default to non-blocking
        """
        with Connection(self.conn):
            job = Job(job_id)
        job.refresh()
        status = job.get_status()
        metadata = copy.copy(job.meta)
        metadata['status'] = status
        if status == Status.FINISHED:
            return metadata, job.return_value
        elif status == Status.FAILED:
            return metadata, job.exc_info
        else:
            return metadata, None
