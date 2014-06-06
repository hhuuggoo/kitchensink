from rq.job import Job, Status
from rq import Queue
from rq import Connection

class TaskQueue(object):
    def __init__(self, queue_names, redis_conn):
        self.conn = redis_conn
        self.queue_names = queue_names
        self.queues = {}
        with Connection(self.conn):
            for x in queue_names:
                self.queues[x] = Queue(x)
            
    def enqueue(self, queue_name, func, args, kwargs, metadata={}):
        job = self.queues[x].enqueue_call(func, args=args, kwargs=kwargs)
        for k,v in metadata:
            job.meta[k] = v
        job.save()
        return job._id
    
    def status(self, job_id, timeout=None):
        """timeout is None - default to non-blocking
        """
        with Connection(self.conn):
            job = Job(job_id)
        job.refresh()
        status = job.get_status()
        if status == Status.FINISHED:
            return status, job.return_value
        elif status == Status.FAILED:
            return status, job.exc_info
        else:
            return status, None
        

        
            
        
