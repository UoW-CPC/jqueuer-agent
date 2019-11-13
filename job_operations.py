from __future__ import absolute_import, unicode_literals
import time
import shlex
import subprocess
import random
import sys
import os

import celery
from celery.exceptions import Reject, WorkerTerminate, WorkerShutdown

import monitoring
import container_worker as jqw
from container_worker import job_app
import logging

# What to do when a job fails
class JQueuer_Task(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print("{0!r} failed: {1!r}".format(task_id, exc))


index = 0
container_dead = False
logger = logging.getLogger(__name__)

# Implementing the add function to start a job execution
@job_app.task(bind=True, acks_late=True, track_started=True, task_reject_on_worker_lost=True, base=JQueuer_Task)  #
def add(self, exp_id, job_queue_id, job):
    global index, container_dead
    if container_dead:
        time.sleep(15)
        raise Reject("my container is dead", requeue=True)
    index = index + 1
    job_params = job["params"]
    job_command = job["command"]
    job_start_time = time.time()
    output = ""

    worker_id = self.request.hostname.split("@")[1]
    logger.info("Worker Id: {0}, Job Id: {1}".format(worker_id, job["id"]))
    monitoring.run_job(
        getNodeID(worker_id), exp_id, getServiceName(worker_id), worker_id, job["id"]
    )
    tasks = job["tasks"]
    try:
        if isinstance(tasks, list):
            output = process_list(worker_id, exp_id, job_queue_id, job, job_start_time)
        else:
            output = process_array(worker_id, exp_id, job_queue_id, job, job_start_time)
        response = monitoring.terminate_job(
            getNodeID(worker_id),
            exp_id,
            getServiceName(worker_id),
            worker_id,
            job["id"],
            job_start_time,
        )
        if response.lower() == "stop_worker":
            self.update_state(state="SUCCESS")
            output = pause_container(worker_id)            
            container_dead = True
            time.sleep(10)
    except subprocess.CalledProcessError as e:
        response = monitoring.job_failed(
            getNodeID(worker_id),
            exp_id,
            getServiceName(worker_id),
            worker_id,
            job["id"],
            job_start_time,
        )
        if response.lower() == "stop_worker":
            self.update_state(state="REVOKED")
            output = pause_container(worker_id)
            logger.info("Terminate job - Pause command output: {0}".format(output))
            container_dead = True
        # self.update_state(state="RETRY")
        time.sleep(10) # Changed from 200 to 10

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

# Stop container
def pause_container(worker_id):
    command = (
            ["docker", "stop", getContainerID(worker_id)]
        )
    output = subprocess.check_output(command)
    if isinstance(output, bytes):
        output = output.decode()
    return output

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

