import time
import sys
import requests
import json
import uuid
import logging

from parameters import jqueuer_service_url

logger = logging.getLogger(__name__)

def run_job(qworker_id, job_id):
    labels = {'metric_type': 'run_job', 'qworker_id': qworker_id, 'job_id':job_id}
    post_metric(labels)

def terminate_retried_job(qworker_id, job_id):
    labels = {'metric_type': 'terminate_retried_job', 'qworker_id': qworker_id, 'job_id':job_id}
    return post_metric(labels)

def terminate_job(qworker_id, job_id, start_time):
    labels = {'metric_type': 'terminate_job', 'qworker_id': qworker_id, 'job_id':job_id, 'start_time': start_time}
    return post_metric(labels)

def job_failed(qworker_id, job_id, fail_time):
    labels = {'metric_type': 'job_failed', 'qworker_id': qworker_id, 'job_id':job_id, 'fail_time': fail_time}
    return post_metric(labels)

def run_task(qworker_id, job_id, task_id):
    labels = {'metric_type': 'run_task', 'qworker_id': qworker_id, 'job_id':job_id, 'task_id': task_id}
    post_metric(labels)

def terminate_task(qworker_id, job_id, task_id, start_time):
    labels = {'metric_type': 'terminate_task', 'qworker_id': qworker_id, 'job_id': job_id, 'task_id': task_id, 'start_time': start_time}
    post_metric(labels)
    
def task_failed(qworker_id, job_id, task_id, fail_time):
    labels = {'metric_type': 'task_failed', 'qworker_id': qworker_id, 'job_id': job_id, 'task_id': task_id, 'fail_time': fail_time}
    post_metric(labels)

def post_metric(data):
    try:
        json_data = json.dumps(data)
        r = requests.post(url = jqueuer_service_url, data = json_data) 
        logger.info(r.text)
        return r.text    
    except Exception as e:
        logger.error(e)
    return ""
    