import copy

from rq.job import Status
from rq import Queue
from rq import Connection

from .objs import KitchenSinkJob, KitchenSinkRedisQueue, pull_intermediate_results

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
        for k,v in metadata.items():
            job.meta[k] = v
        job.save()
        return job._id, job.get_status()

    def bulkstatus(self, job_ids, timeout=5.0):
        with Connection(self.conn):
            jobs = [KitchenSinkJob(job_id) for job_id in job_ids]
        statuses = [job.get_status() for job in jobs]
        if any([x in {Status.FINISHED, Status.FAILED} for x in statuses]):
            messages = pull_intermediate_results(self.conn, job_ids, timeout=0)
        else:
            messages = pull_intermediate_results(self.conn, job_ids, timeout=timeout)
        statuses = [job.get_status() for job in jobs]
        metadata = {}
        results = {}
        messages_by_job = {}
        for job_id, msg in messages:
            messages_by_job.setdefault(job_id, []).append(msg)
        for job_id, job, status in zip(job_ids, jobs, statuses):
            job.refresh()
            mdata = copy.copy(job.meta)
            mdata['status'] = status
            mdata['msgs'] = messages_by_job.get(job_id, [])
            metadata[job_id] = mdata
            if status == Status.FINISHED:
                results[job_id] = job.return_value
            elif status == Status.FAILED:
                results[job_id] = job.exc_info
            else:
                results[job_id] = None
        return [(metadata[job_id], results[job_id]) for job_id in job_ids]

    def status(self, job_id, timeout=None):
        """timeout is None - default to non-blocking
        """
        with Connection(self.conn):
            job = KitchenSinkJob(job_id)
        job.refresh()
        status = job.get_status()
        # if the job is finished, grab all messages and get out!
        if status == Status.FINISHED:
            messages = job.pull_intermediate_results(timeout=0)
        else:
            messages = job.pull_intermediate_results()
            #then refresh the job again
            job.refresh()
            status = job.get_status()
        metadata = copy.copy(job.meta)
        metadata['status'] = status
        metadata['msgs'] = messages

        if status == Status.FINISHED:
            return metadata, job.return_value
        elif status == Status.FAILED:
            return metadata, job.exc_info
        else:
            return metadata, None
