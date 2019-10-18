import time
import sys
import requests
import json
import uuid
import logging

from parameters import jqueuer_service_url

logger = logging.getLogger(__name__)

instance_id = uuid.uuid4()
def add_worker(node_id, experiment_id, service_name):
    labels = {'metric_type': 'add_worker', 'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name}
    post_metric(labels)

def terminate_worker(node_id,experiment_id, service_name):
    labels = {'metric_type': 'terminate_worker', 'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name}
    post_metric(labels)

def run_job(node_id, experiment_id, service_name, qworker_id, job_id):
    labels = {'metric_type': 'run_job', 'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id':job_id}
    post_metric(labels)

def terminate_job(node_id, experiment_id, service_name, qworker_id, job_id, start_time):
    labels = {'metric_type': 'terminate_job', 'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id':job_id, 'start_time': start_time}
    post_metric(labels)

def job_failed(node_id, experiment_id, service_name, qworker_id, job_id, fail_time):
    labels = {'metric_type': 'job_failed', 'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id':job_id, 'fail_time': fail_time}
    post_metric(labels)

def run_task(node_id, experiment_id, service_name, qworker_id, job_id, task_id):
    labels = {'metric_type': 'run_task', 'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id':job_id, 'task_id': task_id}
    post_metric(labels)

def terminate_task(node_id, experiment_id, service_name, qworker_id, job_id, task_id, start_time):
    labels = {'metric_type': 'terminate_task', 'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id': job_id, 'task_id': task_id, 'start_time': start_time}
    post_metric(labels)
    
def task_failed(node_id, experiment_id, service_name, qworker_id, job_id, task_id, fail_time):
    labels = {'metric_type': 'task_failed', 'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id': job_id, 'task_id': task_id, 'fail_time': fail_time}
    post_metric(labels)

def post_metric(data):
    logger.info("Post metric - The metric type is: {}, where the labels are: {}".format(metric_type,labels))
    logger.info("Post metric - The url is: {}".format(jqueuer_service_url))
    try:
        json_data = json.dumps(data)
        r = requests.post(url = jqueuer_service_url, data = json_data) 
        logger.info(r.text)
        return r.text    
    except Exception as e:
        logger.error(e)
    return ""
    