#!/usr/bin/env python3

import sys
import urllib.request as request
import json


def print_usage():
    print("Usage:")
    print("  e.g.  dtle-7to11.py 'http://127.0.0.1:4646'")


if len(sys.argv) != 2:
    print_usage()
    exit(0)

if not sys.argv[1].startswith("http"):
    print_usage()
    exit(0)

nomad_url = sys.argv[1]

resp = request.urlopen(nomad_url + "/v1/jobs")
j_jobs = json.load(resp)
resp.close()

for job in j_jobs:
    job_id = job['ID']
    resp = request.urlopen(nomad_url + "/v1/job/" + job_id)
    j_job = json.load(resp)
    resp.close()

    src_task = None
    dest_task = None

    # get src/dest tasks
    for task_group in j_job["TaskGroups"]:
        for task in task_group["Tasks"]:
            if task["Name"] == "src":
                src_task = task
            elif task["Name"] == "dest":
                dest_task = task

    if src_task is None or dest_task is None:
        print("failed to convert job: %s" % job_id)
    else:
        src_config = src_task["Config"]
        dest_config = dest_task["Config"]
        if "DestType" in dest_config:
            print("job already converted: %s" % job_id)
        else:
            # rename src ConnectionConfig
            if "ConnectionConfig" in src_config:
                src_config["SrcConnectionConfig"] = src_config["ConnectionConfig"]
                del src_config["ConnectionConfig"]
            # move dest config to src
            for k in dest_config:
                new_key = "DestConnectionConfig" if k == "ConnectionConfig" else k
                src_config[new_key] = dest_config[k]
            # dest special config
            is_kafka = "KafkaConfig" in src_config
            dest_task["Config"] = {"DestType": "kafka" if is_kafka else "mysql"}

            # POST job
            payload = json.dumps({"Job": j_job}).encode("utf-8")
            req = request.Request(nomad_url + "/v1/jobs",
                                  data=payload, method='POST')
            with request.urlopen(req) as resp:
                print("job converted: %s %s" % (job_id, resp.msg))
