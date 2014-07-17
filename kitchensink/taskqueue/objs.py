"""module for rq subclasses
"""
from rq import Queue
from rq.job import Job, UNEVALUATED
import dill


class KitchenSinkJob(Job):
    pass

class KitchenSinkRedisQueue(Queue):
    job_class = KitchenSinkJob
