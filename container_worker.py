from __future__ import absolute_import, unicode_literals
from threading import Thread
import time
import sys
import ast

from celery import Celery
from celery.bin import worker

import parameters as _params

worker_name = "NoName"


def init(service_name):
    job_app = Celery(
        "job_app",
        broker=_params.broker(),
        backend=_params.backend(1),
        include=["job_operations"],
    )

    job_app.conf.update(
        task_routes={
            "job_operations.add": {"queue": _params.JOB_QUEUE_PREFIX + service_name}
        },
        task_default_queue="job_default_queue" + "_" + service_name,
        result_expires=3600,
        task_serializer="json",
        accept_content=["json"],
        worker_concurrency=1,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_default_exchange="job_exchange" + "_" + service_name,
        task_default_routing_key="job_routing_key" + "_" + service_name,
    )
    return job_app


job_app = init("")

if __name__ == "__main__":
    node_id = sys.argv[1]
    container = ast.literal_eval(sys.argv[2])
    worker_id = node_id + "##" + container["service_name"] + "##" + container["id_long"]

    job_app = init(container["service_name"])
    job_worker = worker.worker(app=job_app)
    job_options = {
        "hostname": worker_id,
        "queues": [_params.JOB_QUEUE_PREFIX + container["service_name"]],
        "loglevel": "INFO",
        "traceback": True,
    }
    job_worker.run(**job_options)

