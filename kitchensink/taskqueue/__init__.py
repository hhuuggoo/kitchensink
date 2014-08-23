import copy
import time
import logging

from rq.job import Status
from rq import Queue, cancel_job
from rq import Connection

from .objs import KitchenSinkJob, KitchenSinkRedisQueue, pull_intermediate_results

log = logging.getLogger(__name__)

class TaskQueue(object):
    def __init__(self, redis_conn):
        self.conn = redis_conn
        self.queues = {}

    def get_queue(self, name):
        if not name in self.queues:
            log.info("MAKING QUEUE")
            queue = KitchenSinkRedisQueue(name, connection=self.conn)
            self.queues[name] = queue
        return self.queues[name]

    def make_job(self, func, args, kwargs, metadata={}):
        job = KitchenSinkJob.create(
            func, args, kwargs, connection=self.conn,
            result_ttl=None, status=Status.QUEUED,
            description="ksjob", depends_on=None, timeout=None)
        for k,v in metadata.items():
            job.meta[k] = v
        job.save()
        return job

    def enqueue(self, queue_names, func, args, kwargs, metadata={}):
        st = time.time()
        job = self.make_job(func, args, kwargs, metadata=metadata)
        for queue_name in queue_names:
            queue = self.get_queue(queue_name)
            queue.enqueue_job(job)
        ed = time.time()
        log.info("TOTAL enqueued job %s", ed-st)
        return job._id, job.get_status()

    # def enqueue(self, queue_name, func, args, kwargs, metadata={}):
    #     queue = self.get_queue(queue_name)
    #     job = self.get_queue(queue_name).enqueue_call(func, args=args, kwargs=kwargs)
    #     for k,v in metadata.items():
    #         job.meta[k] = v
    #     job.save()
    #     return job._id, job.get_status()

    def bulkstatus(self, job_ids, timeout=5.0):
        st = time.time()
        with Connection(self.conn):
            jobs = [KitchenSinkJob(job_id) for job_id in job_ids]
        statuses = [job.get_status() for job in jobs]
        if any([x in {Status.FINISHED, Status.FAILED} for x in statuses]):
            messages = pull_intermediate_results(self.conn, job_ids, timeout=0)
        else:
            messages = pull_intermediate_results(self.conn, job_ids, timeout=timeout)
        mt = time.time()
        print ("MESSAGES %s, %s, %s" % (mt, mt - st, messages))
        statuses = [job.get_status() for job in jobs]
        metadata = {}
        results = {}
        messages_by_job = {}
        for job_id, msg in messages:
            messages_by_job.setdefault(job_id, []).append(msg)
        for job_id, job, status in zip(job_ids, jobs, statuses):
            if status == Status.FINISHED or status == Status.FAILED:
                job.refresh()
                mdata = copy.copy(job.meta)
            else:
                mdata = {}
            mdata['status'] = status
            mdata['msgs'] = messages_by_job.get(job_id, [])
            metadata[job_id] = mdata
            if status == Status.FINISHED:
                results[job_id] = job.return_value
                job.delete()
            elif status == Status.FAILED:
                results[job_id] = job.exc_info
                job.delete()
            else:
                results[job_id] = None
        ed = time.time()
        log.debug("queue BULKSTATUS %s", ed - st)
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
            retval = job.return_value
            job.delete()
            log.info("delete job")
            return metadata, retval

        elif status == Status.FAILED:
            retval = job.exc_info
            job.delete()
            log.info("delete job")
            return metadata, retval
        else:
            return metadata, None
    def cancel(self, job_id):
        with Connection(self.conn):
            cancel_job(job_id)
