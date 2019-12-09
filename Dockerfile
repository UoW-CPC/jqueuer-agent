FROM python:3.6-slim

WORKDIR /var/lib/jqueuer-worker/
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . .
RUN apt update \
&& apt install -y libltdl7 \
&& mkdir log \
&& mkdir data \
&& rm requirements.txt \
&& rm -rf /var/lib/apt/lists/*

ENTRYPOINT python3 jqueuer_agent.py