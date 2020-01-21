from __future__ import absolute_import, unicode_literals
import time
import shlex
import subprocess
import random
import sys
import os

import celery
from celery.exceptions import Reject, WorkerTerminate, WorkerShutdown
from billiard.einfo import ExceptionInfo
from celery.signals import task_rejected

import monitoring
from parameters import jqueuer_job_max_retries
import container_worker as jqw
from container_worker import job_app
import logging

# What to do when a job fails
class JQueuer_Task(celery.Task):
    jqueuer_job_start_time = -1
    jqueuer_job = {}

    def reject_callback(self,args, kwargs):
        logger.info("Rejected")
    

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        global container_dead
        # Data for metric
        worker_id = self.request.hostname.split("@")[1]
        exp_id = args[0]
        # job = args[2]
        # Log message
        log_message = ('\non_failure: Task Id: {0} \n exp_id: {1} \t worker_id: {2} \t job_id: {3} \n retries: {4} \t start_time: {5}'
                    ).format(task_id, exp_id, worker_id, self.jqueuer_job["id"], self.request.retries, self.jqueuer_job_start_time)
        logger.info(log_message)
        # send metric
        response = monitoring.job_failed(worker_id, exp_id, self.jqueuer_job["id"],self.jqueuer_job_start_time)
        if response.lower() == "stop_worker":
            container_dead = True
            pause_output = pause_container(worker_id)
            logger.info("\non_failure - Pause command output: {0}".format(pause_output))
            time.sleep(300) 

    def on_retry(self,exc, task_id, args, kwargs, einfo):
        global container_dead
        # Log message
        worker_id = self.request.hostname.split("@")[1]
        exp_id = args[0]

        log_message = ('\non_retry: Task Id: {0} \n exp_id: {1} \t worker_id: {2} \t job_id: {3} \n retries: {4} \t start_time: {5}'
                    ).format(task_id, exp_id , worker_id, self.jqueuer_job["id"], self.request.retries, self.jqueuer_job_start_time)
        logger.info(log_message)
        # send metric
        response = monitoring.terminate_retried_job(worker_id, exp_id, self.jqueuer_job["id"])
        if response.lower() == "stop_worker":
            container_dead = True
            pause_output = pause_container(worker_id)
            logger.info("\non_retry job - Pause command output: {0}".format(pause_output))
            time.sleep(300)
    
    def on_success(self,retval, task_id, args, kwargs):
        global container_dead
        worker_id = self.request.hostname.split("@")[1]
        exp_id = args[0]
        # Log message
        log_message = ('\non_success: Task Id: {0} \n exp_id: {1} \t worker_id: {2} \t job_id: {3} \t start_time: {4}'
                    ).format(task_id, exp_id , worker_id, self.jqueuer_job["id"], self.jqueuer_job_start_time)
        logger.info(log_message)
        # send metric
        response = monitoring.terminate_job(worker_id, exp_id, self.jqueuer_job["id"], self.jqueuer_job_start_time)
        if response.lower() == "stop_worker":
            container_dead = True
            pause_output = pause_container(worker_id)
            logger.info("\non_success - Pause command output: {0}".format(pause_output))
            time.sleep(300)
    
container_dead = False
logger = logging.getLogger(__name__)


# Implementing the add function to start a job execution
@job_app.task(bind=True, acks_late=True, autoretry_for=(subprocess.CalledProcessError,), retry_kwargs={'max_retries': jqueuer_job_max_retries, 'countdown': 10}, track_started=True, task_reject_on_worker_lost=True, base=JQueuer_Task)  #
def add(self, exp_id, job_queue_id, job):
    global container_dead
    
    worker_id = self.request.hostname.split("@")[1]
    if container_dead:
        time.sleep(15)
        logger.info("Container dead - Worker Id {0}, Job Id {1}".format(worker_id,job["id"]))
        raise Reject("my container is dead", requeue=True)
    # Update class level variables.
    self.jqueuer_job_start_time = time.time()
    self.jqueuer_job = job 

    output = ""
    logger.info("Task add - Worker Id: {0} \t Job Id: {1} \t Start time: {2}".format(worker_id, job["id"],self.jqueuer_job_start_time))
    monitoring.run_job(worker_id, exp_id, job["id"])
    tasks = job["tasks"]
    if isinstance(tasks, list):
        output = process_list(worker_id, exp_id, job_queue_id, job, self.jqueuer_job_start_time)
    else:
        output = process_array(worker_id, exp_id, job_queue_id, job, self.jqueuer_job_start_time)
    
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


# Pause container
def pause_container(worker_id):
    command = (
            ["docker", "pause", getContainerID(worker_id)]
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
        monitoring.run_task(worker_id, exp_id, job["id"],task["id"])

        command = (
            ["docker", "exec", getContainerID(worker_id)]
            + task_command
            + task["data"]
        )
        output = subprocess.check_output(command)
        monitoring.terminate_task(worker_id, exp_id, job["id"],task["id"],task_start_time)

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
        monitoring.run_task(worker_id, exp_id, job["id"], task_id)
        command = (
            ["docker", "exec", getContainerID(worker_id)]
            + tasks["command"]
            + [str(tasks["data"])]
        )
        output = subprocess.check_output(command)
        monitoring.terminate_task(worker_id, exp_id, job["id"], task_id, task_start_time)

    # A post-job script might be added here

    return output

