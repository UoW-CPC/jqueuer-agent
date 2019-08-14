import time
import sys

from prometheus_client import start_http_server, Gauge, Counter, CollectorRegistry, push_to_gateway
from parameters import pushgateway_url

# Container relevant address for prometheous metrics export
METRICS_FILE = 'jqueuer-agent-metrics.prom' 
print("pushgateway_url:" + pushgateway_url)

registry = CollectorRegistry()
    # Number of workers
JQUEUER_WORKER_COUNT = "jqueuer_worker_count"
node_counter = Counter(JQUEUER_WORKER_COUNT, "JQueuer Worker", ["node_id", "service_name"],registry=registry)


def add_worker(node_id, experiment_id, service_name):
    node_counter.labels(node_id,service_name).inc()
    push_to_gateway(pushgateway_url, job=experiment_id, registry=registry)
    write_to_textfile(METRICS_FILE, registry)


def terminate_worker(node_id,experiment_id, service_name):
    node_counter.labels(node_id,service_name).dec()
    push_to_gateway(pushgateway_url, job=experiment_id, registry=registry)
    #write_to_textfile(METRICS_FILE, registry)


# Running a specific job
JQUEUER_JOB_STARTED_TIMESTAMP = "jqueuer_job_started_timestamp"
JQUEUER_JOB_RUNNING_TIMESTAMP = "jqueuer_job_running_timestamp"
JQUEUER_JOB_RUNNING = "jqueuer_job_running"
JQUEUER_JOB_STARTED = "jqueuer_job_started"

job_started_timestamp = Gauge(JQUEUER_JOB_STARTED_TIMESTAMP,JQUEUER_JOB_STARTED_TIMESTAMP,["node_id","experiment_id","service_name","job_id"],registry=registry)
job_running_timestamp = Gauge(JQUEUER_JOB_RUNNING_TIMESTAMP,JQUEUER_JOB_RUNNING_TIMESTAMP,["node_id","experiment_id","service_name","job_id"],registry=registry)
job_running = Gauge(JQUEUER_JOB_RUNNING,JQUEUER_JOB_RUNNING,["node_id","experiment_id","service_name","qworker_id","job_id"],registry=registry)
job_started = Gauge(JQUEUER_JOB_STARTED,JQUEUER_JOB_STARTED,["node_id","experiment_id","service_name","qworker_id","job_id"],registry=registry)

def run_job(node_id, experiment_id, service_name, qworker_id, job_id):
    job_started_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_running_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_running.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(1)
    job_started.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(1)
    push_to_gateway(pushgateway_url, job=experiment_id, registry=registry)
    #write_to_textfile(METRICS_FILE, registry)

# A specific job is accomplished
JQUEUER_JOB_ACCOMPLISHED_TIMESTAMP = "jqueuer_job_accomplished_timestamp"
JQUEUER_JOB_ACCOMPLISHED_DURATION = "jqueuer_job_accomplished_duration"
JQUEUER_JOB_ACCOMPLISHED = "jqueuer_job_accomplished"

job_accomplished_timestamp = Gauge(JQUEUER_JOB_ACCOMPLISHED_TIMESTAMP,JQUEUER_JOB_ACCOMPLISHED_TIMESTAMP,["node_id","experiment_id","service_name","job_id"],registry=registry)
job_accomplished_duration = Gauge(JQUEUER_JOB_ACCOMPLISHED_DURATION,JQUEUER_JOB_ACCOMPLISHED_DURATION,["node_id","experiment_id","service_name","job_id"],registry=registry)
job_accomplished = Gauge(JQUEUER_JOB_ACCOMPLISHED,JQUEUER_JOB_ACCOMPLISHED,["node_id","experiment_id","service_name","qworker_id","job_id"],registry=registry)

def terminate_job(node_id, experiment_id, service_name, qworker_id, job_id, start_time):
    elapsed_time = time.time() - start_time
    job_accomplished_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    #job_running_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_accomplished_duration.labels(node_id,experiment_id,service_name,job_id).set(elapsed_time)
    job_accomplished.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(1)
    job_running.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(0)
    push_to_gateway(pushgateway_url, job=experiment_id, registry=registry)
    #write_to_textfile(METRICS_FILE, registry)

# A specific job is failed
JQUEUER_JOB_FAILED_TIMESTAMP = "jqueuer_job_failed_timestamp"
JQUEUER_JOB_FAILED_DURATION = "jqueuer_job_failed_duration"
JQUEUER_JOB_FAILED = "jqueuer_job_failed"

job_failed_timestamp = Gauge(JQUEUER_JOB_FAILED_TIMESTAMP,JQUEUER_JOB_FAILED_TIMESTAMP,["node_id","experiment_id","service_name","job_id"],registry=registry)
job_failed_duration = Gauge(JQUEUER_JOB_FAILED_DURATION,JQUEUER_JOB_FAILED_DURATION,["node_id","experiment_id","service_name","job_id"],registry=registry)
job_failed_ga = Gauge(JQUEUER_JOB_FAILED,JQUEUER_JOB_FAILED,["node_id","experiment_id","service_name","qworker_id","job_id"],registry=registry)

def job_failed(node_id, experiment_id, service_name, qworker_id, job_id, fail_time):
    elapsed_time = time.time() - fail_time
    job_failed_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    #job_running_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_failed_duration.labels(node_id,experiment_id,service_name,job_id).set(elapsed_time)
    job_failed_ga.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(1)
    job_running.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(0)    
    push_to_gateway(pushgateway_url, job=experiment_id, registry=registry)
    #write_to_textfile(METRICS_FILE, registry)

# A specific task is started
JQUEUER_TASK_STARTED_TIMESTAMP = "jqueuer_task_started_timestamp"
JQUEUER_TASK_RUNNING_TIMESTAMP = "jqueuer_task_running_timestamp"
JQUEUER_TASK_RUNNING = "jqueuer_task_running"
JQUEUER_TASK_STARTED = "jqueuer_task_started"

task_started_timestamp = Gauge(JQUEUER_TASK_STARTED_TIMESTAMP,JQUEUER_TASK_STARTED_TIMESTAMP,["node_id","experiment_id","service_name","job_id","task_id"],registry=registry)
task_running_timestamp = Gauge(JQUEUER_TASK_RUNNING_TIMESTAMP,JQUEUER_TASK_RUNNING_TIMESTAMP,["node_id","experiment_id","service_name","job_id","task_id"],registry=registry) 
task_running = Gauge(JQUEUER_TASK_RUNNING,JQUEUER_TASK_RUNNING,["node_id","experiment_id","service_name","qworker_id","job_id","task_id"],registry=registry)
task_started = Gauge(JQUEUER_TASK_STARTED,JQUEUER_TASK_STARTED,["node_id","experiment_id","service_name","qworker_id","job_id","task_id"],registry=registry)

def run_task(node_id, experiment_id, service_name, qworker_id, job_id, task_id):
    task_started_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_running_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_running.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(1)
    task_started.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(1) 
    push_to_gateway(pushgateway_url, job=experiment_id, registry=registry)
    #write_to_textfile(METRICS_FILE, registry)

# A specific task is accomplished
JQUEUER_TASK_ACCOMPLISHED_TIMESTAMP = "jqueuer_task_accomplished_timestamp"
JQUEUER_TASK_ACCOMPLISHED_DURATION = "jqueuer_task_accomplished_duration"
JQUEUER_TASK_ACCOMPLISHED = "jqueuer_task_accomplished"

task_accomplished_timestamp = Gauge(JQUEUER_TASK_ACCOMPLISHED_TIMESTAMP,JQUEUER_TASK_ACCOMPLISHED_TIMESTAMP,["node_id","experiment_id","service_name","job_id","task_id"],registry=registry)
task_accomplished_duration = Gauge(JQUEUER_TASK_ACCOMPLISHED_DURATION,JQUEUER_TASK_ACCOMPLISHED_DURATION,["node_id","experiment_id","service_name","job_id","task_id"],registry=registry)
task_accomplished = Gauge(JQUEUER_TASK_ACCOMPLISHED,JQUEUER_TASK_ACCOMPLISHED,["node_id","experiment_id","service_name","qworker_id","job_id","task_id"],registry=registry)

def terminate_task(
    node_id, experiment_id, service_name, qworker_id, job_id, task_id, start_time
):
    elapsed_time = time.time() - start_time
    task_accomplished_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_accomplished_duration.labels(node_id,experiment_id,service_name,job_id,task_id).set(elapsed_time)
    task_accomplished.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(1)
    task_running.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(0) 
    push_to_gateway(pushgateway_url, job=experiment_id, registry=registry)
    #write_to_textfile(METRICS_FILE, registry)

# Task failed
JQUEUER_TASK_FAILED_TIMESTAMP = "jqueuer_task_failed_timestamp"
JQUEUER_TASK_FAILED_DURATION = "jqueuer_task_failed_duration"
JQUEUER_TASK_FAILED = "jqueuer_task_failed"

task_failed_timestamp = Gauge(JQUEUER_TASK_FAILED_TIMESTAMP,JQUEUER_TASK_FAILED_TIMESTAMP,["node_id","experiment_id","service_name","job_id","task_id"],registry=registry)
task_failed_duration = Gauge(JQUEUER_TASK_FAILED_DURATION,JQUEUER_TASK_FAILED_DURATION,["node_id","experiment_id","service_name","qworker_id","job_id","task_id"],registry=registry)
task_failed_ga = Gauge(JQUEUER_TASK_FAILED,JQUEUER_TASK_FAILED,["node_id","experiment_id","service_name","qworker_id","job_id","task_id"],registry=registry)

def task_failed(
    node_id, experiment_id, service_name, qworker_id, job_id, task_id, fail_time
):
    elapsed_time = time.time() - fail_time
    task_failed_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_failed_duration.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(elapsed_time)
    task_failed_ga.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(1)
    task_running.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(0)
    push_to_gateway(pushgateway_url, job=experiment_id, registry=registry)
    #write_to_textfile(METRICS_FILE, registry)