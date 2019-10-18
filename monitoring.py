import time
import sys
import requests
import json
import uuid

from parameters import jqueuer_service_url
instance_id = uuid.uuid4()
def add_worker(node_id, experiment_id, service_name):
    labels = {'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name}
    post_metric("add_worker",labels)

def terminate_worker(node_id,experiment_id, service_name):
    labels = {'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name}
    post_metric("terminate_worker",labels)

def run_job(node_id, experiment_id, service_name, qworker_id, job_id):
    labels = {'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id':job_id}
    post_metric("run_job",labels)

def terminate_job(node_id, experiment_id, service_name, qworker_id, job_id, start_time):
    labels = {'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id':job_id, 'start_time': start_time}
    post_metric("terminate_job",labels)

def job_failed(node_id, experiment_id, service_name, qworker_id, job_id, fail_time):
    labels = {'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id':job_id, 'fail_time': fail_time}
    post_metric("job_failed",labels)

def run_task(node_id, experiment_id, service_name, qworker_id, job_id, task_id):
    labels = {'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id':job_id, 'task_id': task_id}
    post_metric("run_task",labels)

def terminate_task(node_id, experiment_id, service_name, qworker_id, job_id, task_id, start_time):
    labels = {'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id': job_id, 'task_id': task_id, 'start_time': start_time}
    post_metric("terminate_task",labels)
    
def task_failed(node_id, experiment_id, service_name, qworker_id, job_id, task_id, fail_time):
    labels = {'node_id': node_id, 'experiment_id': experiment_id, 'service_name': service_name, 'qworker_id': qworker_id, 'job_id': job_id, 'task_id': task_id, 'fail_time': fail_time}
    post_metric("task_failed",labels)

def post_metric(metric_type,labels):
    data = {}
    data['metric_type'] = metric_type
    data['labels']=labels
    json_data = json.dumps(data)
    r = requests.post(url = jqueuer_service_url, data = json_data) 
    return r.text