from __future__ import absolute_import, unicode_literals
import time
import shlex
import subprocess
import random
import sys
import os

import celery
from celery.exceptions import Reject, WorkerTerminate

import monitoring
import container_worker as jqw
from container_worker import job_app

# What to do when a job fails
class JQueuer_Task(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print("{0!r} failed: {1!r}".format(task_id, exc))
        #time.sleep(30)


index = 0
container_dead = False

# Implementing the add function to start a job execution
@job_app.task(bind=True, acks_late=True, track_started=True, task_reject_on_worker_lost=True, base=JQueuer_Task)
def add(self, exp_id, job_queue_id, job):
    global index, container_dead
    if container_dead:
        time.sleep(30)
        raise WorkerTerminate()
        #raise Reject("my container is dead", requeue=True)
    index = index + 1
    job_params = job["params"]
    job_command = job["command"]
    job_start_time = time.time()
    output = ""

    worker_id = self.request.hostname.split("@")[1]

    monitoring.run_job(
        getNodeID(worker_id), exp_id, getServiceName(worker_id), worker_id, job["id"]
    )

    tasks = job["tasks"]
    try:
        if isinstance(tasks, list):
            output = process_list(worker_id, exp_id, job_queue_id, job, job_start_time)
        else:
            output = process_array(worker_id, exp_id, job_queue_id, job, job_start_time)
        monitoring.terminate_job(
            getNodeID(worker_id),
            exp_id,
            getServiceName(worker_id),
            worker_id,
            job["id"],
            job_start_time,
        )
    except subprocess.CalledProcessError as e:
        monitoring.job_failed(
            getNodeID(worker_id),
            exp_id,
            getServiceName(worker_id),
            worker_id,
            job["id"],
            job_start_time,
        )
        container_dead = True
        self.update_state(state="RETRY")
        raise WorkerTerminate()
        #time.sleep(60) # Changed from 200 to 60

    return output


# Get Worker ID
def getNodeID(worker_id):
    return worker_id.split("##")[0]


# Get Service Name
def getServiceName(worker_id):
    return worker_id.split("##")[1]


# Get Container ID
def getContainerID(worker_id):
    return worker_id.split("##")[2]


# Process a list of tasks
def process_list(worker_id, exp_id, job_queue_id, job, job_start_time):
    output = ""

    # A pre-job script might be added here

    # Go through the tasks, execute them sequntially
    for task in job["tasks"]:
        try:
            task_command = task["command"]
        except Exception as e:
            task["command"] = job["command"]
        try:
            task_data = task["data"]
        except Exception as e:
            task["data"] = job["data"]

        task_start_time = time.time()
        monitoring.run_task(
            getNodeID(worker_id),
            exp_id,
            getServiceName(worker_id),
            worker_id,
            job["id"],
            task["id"],
        )

        command = (
            ["docker", "exec", getContainerID(worker_id)]
            + task_command
            + task["data"]
        )
        output = subprocess.check_output(command)
        monitoring.terminate_task(
            getNodeID(worker_id),
            exp_id,
            getServiceName(worker_id),
            worker_id,
            job["id"],
            task["id"],
            task_start_time,
        )

    # A post-job script might be added here


    if isinstance(output, bytes):
        output = output.decode()

    return output


# Process an array of tasks
def process_array(worker_id, exp_id, job_queue_id, job, job_start_time):
    output = ""
    tasks = job["tasks"]
    try:
        task_command = tasks["command"]
    except Exception as e:
        tasks["command"] = job["command"]
    try:
        task_data = tasks["data"]
    except Exception as e:
        tasks["data"] = job["data"]

    # A pre-job script might be added here

    # Go through the tasks, execute them sequntially

    for x in range(0, tasks["count"]):
        task_start_time = time.time()
        task_id = tasks["id"] + "_" + str(x)
        monitoring.run_task(
            getNodeID(worker_id),
            exp_id,
            getServiceName(worker_id),
            worker_id,
            job["id"],
            task_id,
        )
        command = (
            ["docker", "exec", getContainerID(worker_id)]
            + tasks["command"]
            + [str(tasks["data"])]
        )
        output = subprocess.check_output(command)
        monitoring.terminate_task(
            getNodeID(worker_id),
            exp_id,
            getServiceName(worker_id),
            worker_id,
            job["id"],
            task_id,
            task_start_time,
        )

    # A post-job script might be added here

    return output

