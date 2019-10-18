import os

import redis

# Job Queue Prefix
JOB_QUEUE_PREFIX = "jqueue_service_"

# Backend configuration - Rabbitmq
broker_protocol = "pyamqp"
broker_username = os.getenv("RABBIT_USER", "admin")
broker_password = os.getenv("RABBIT_PASS", "mypass")
broker_server = os.getenv("RABBIT_SERVER", "jqueuer-rabbit")
broker_port = os.getenv("RABBIT_PORT", 5672)

# Pushgateway configuration
PUSHGATEWAY_SERVER = os.getenv("PUSHGATEWAY_SERVER", "jqueuer-pushgateway")
PUSHGATEWAY_PORT = os.getenv("PUSHGATEWAY_PORT", "9091")
pushgateway_url = PUSHGATEWAY_SERVER + ":" + PUSHGATEWAY_PORT

# JQueuer Manager Service configuration
JQUEUER_SERVER = os.getenv("JQUEUER_MANAGER_SERVICE_NAME", "jqueuer-manager")
JQUEUER_PORT = os.getenv("JQUEUER_MANAGER_SERVICE_PORT", "8081")
jqueuer_service_url = JQUEUER_SERVER + ":" + JQUEUER_PORT + "/experiment/metrics"

def broker():
    broker = broker_protocol + "://" + broker_username
    if broker_password != "":
        broker = broker + ":" + broker_password
    broker = broker + "@" + broker_server + ":" + str(broker_port) + "//"
    return broker


# Redis Backend configuration
backend_protocol = "redis"
backend_server = os.getenv("REDIS_SERVER", "jqueuer-redis")
backend_password = os.getenv("REDIS_PASS", "mypass")
backend_port = os.getenv("REDIS_PORT", 6379)
backend_db = 0
backend_experiment_db_id = 10

backend_experiment_db = redis.Redis(
    host=backend_server,
    port=backend_port,
    password=backend_password,
    db=backend_experiment_db_id,
    charset="utf-8",
    decode_responses=True,
)


def backend(db):
    backend = (
        backend_protocol
        + "://:"
        + backend_password
        + "@"
        + backend_server
        + ":"
        + str(backend_port)
        + "/"
        + str(db)
    )
    return backend
