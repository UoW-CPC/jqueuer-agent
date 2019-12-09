from __future__ import absolute_import, unicode_literals
from threading import Thread
import shlex
import sys
import time
import subprocess
import ast
import os
import signal
import logging
import uuid
import docker
import redis

from parameters import backend_experiment_db, node_id
import monitoring

""" Configure logging """
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
conlog = logging.StreamHandler()
conlog.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
conlog.setFormatter(formatter)
logger.addHandler(conlog)
logger.info("jQueuer started")

job_workers = {}


def worker(container, node_id):
    
    # Start the app in a new process
    process = subprocess.Popen(
        ["python3", "container_worker.py", str(node_id), str(container)]
    )
    container["process"] = process


# Start the jqueuer_agent process
def start(node_id):
    # A list of controlled containers on the same node
    container_list = {}

    # Docker client
    client = None
    try:
        client = docker.from_env()
    except Exception:
        logger("Could not connect to Docker at /var/run/docker.sock")

    # a counter to trace the dead containers
    current_update = 0

    while True:
        current_update += 1
        # Loop over the containers list
        for container in client.containers.list():
            container_obj = {}
            try:
                container_state_running = container.attrs["State"]["Running"]
                if not container_state_running:
                    continue
                # Get container ID
                container_long_id = container.id

                # Get container's service name
                container_service_name = (
                    container.labels.get("com.docker.swarm.service.name")
                    or container.labels.get("io.kubernetes.container.name")
                    or container.name
                )

                # Check if the service belongs to an experiment
                if not backend_experiment_db.exists(container_service_name):
                    continue

                # Get the experiment id
                [experiment_id] = backend_experiment_db.hmget(
                    container_service_name, "experiment_id"
                )
                # Check if the container has been already added, if yes, update the current_update
                if container_long_id in container_list:
                    container_list[container_long_id]["current_update"] = current_update
                    continue

                container_obj = {
                    "id_long": container_long_id,
                    "name": container.attrs["Name"],
                    "service_name": container_service_name,
                    "task_id": container.labels.get("com.docker.swarm.task.id")
                    or container.labels.get("io.kubernetes.pod.uid"),
                    "task_name": container.labels.get("com.docker.swarm.task.name")
                    or container.labels.get("io.kubernetes.pod.name"),
                    "hostname": container.attrs["Config"]["Hostname"],
                    "ip_address": "",
                    "created": container.attrs["Created"],
                    "started": container.attrs["State"]["StartedAt"],
                    "experiment_id": experiment_id,
                    "current_update": current_update,
                }
                container_obj["ip_address"] = (
                    container.attrs["NetworkSettings"]["Networks"]
                    .get("bridge", {})
                    .get("IPAddress")
                )

                try:
                    # Start a new thread to control this container
                    job_worker_thread = Thread(
                        target=worker, args=(container_obj, node_id)
                    )
                    job_worker_thread.start()
                    container_list[container_long_id] = container_obj

                except Exception as e:
                    logger.error(
                        "Error starting thread for {}".format(
                            container.name
                        )
                    )
                    logger.debug(e)
            except Exception as e:
                logger.error(
                    "Error getting container info for {}".format(
                        container.name
                    )
                )
                logger.debug(e)
        try:
            # Containers that should be deleted from the list because they aren't alive
            trash = []

            # Trash the containers which weren't updated in the last three rounds
            for container_id_temp in container_list:
                container_temp = container_list[container_id_temp]
                if current_update - container_temp["current_update"] > 2:
                    os.killpg(os.getpgid(container_temp["process"].pid), signal.SIGTERM)
                    trash.append(container_id_temp)
            for x in trash:
                del container_list[x]
        except Exception as e:
            logger.debug("Error with garbage collection...")
            time.sleep(5)

        time.sleep(5)


if __name__ == "__main__":
    if node_id == "default_id_1":
        logger.error("The jqueuer agent node IP must be provided.")
    else:
        # start jqueuer agent process
        logger.info ("The provided jqueuer agent IP address is: {0}".format(node_id))
        start(node_id)


