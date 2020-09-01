# coding=utf-8
"""
    Name:       fromdtle2nomad
    Author:     Andy Liu
    Email :     liuan@actionsky.com
    Created:    2020/8/20
    Requirement package: requests <pip install requests>
"""
import requests
import logging
import json
import sys
from pprint import pformat
from copy import deepcopy

# CHANGE HERE
#DTLE_IP = "127.0.0.1"
#DTLE_PORT = "8190"

LOGGER = logging.getLogger()

TASK_DROP_ITEMS = ['ConfigLock', 'Constraints', 'Leader', 'NodeID', 'NodeName']
NOMAD_SRC_CFG_WHITE_LIST = ['Gtid', 'GtidStart', 'AutoGtid', 'BinlogRelay', 'BinlogFile', 'BinlogPos',
                            'ReplicateDoDb', 'ConnectionConfig', 'DropTableIfExists', 'SkipCreateDbTable',
                            'ReplChanBufferSize', 'ChunkSize', 'ExpandSyntaxSupport', 'GroupMaxSize', 'GroupTimeout',
                            'SqlFilter']
NOMAD_DEST_CFG_WHITE_LIST = ['ConnectionConfig', 'ParallelWorkers', 'Brokers', 'Converter', 'Topic', 'TimeZone']
# fix kafka job return float
NEED_INT = ['BinlogPos', 'GroupMaxSize', 'ChunkSize', 'GroupTimeout', 'ReplChanBufferSize', 'ParallelWorkers']


def init_log():
    log_format = "[%(asctime)s][%(levelname)5s][%(filename)14s][%(lineno)4d][%(funcName)s][%(message)s]"
    dft = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format=log_format, level=logging.DEBUG, datefmt=dft, filename='fromdtle2nomad.log',
                        filemode='w')


def get_jobs(DTLE_IP , DTLE_PORT):
    api = 'http://{0}:{1}/v1/jobs'.format(DTLE_IP, DTLE_PORT)
    LOGGER.debug('api url: <{0}>'.format(pformat(api)))
    r = requests.get(api)
    if r.status_code != requests.codes.ok:
        LOGGER.error('GET <{0}> - status_code: <{1}> - content: <{2}>'.format(api, r.status_code, r.text))
        exit(1)
    jobs = r.json()
    LOGGER.debug('jobs: <{0}>'.format(pformat(jobs)))
    return jobs


def parse(jobs):
    for job in jobs:
        new_job = {'ID': job['ID'],
                   'Name': job['Name'],
                   'Datacenters': job['JobSummary']['Datacenters'],
                   'Failover': job["JobSummary"]["Failover"],
                   'Region': job["JobSummary"]["Region"],
                   'Tasks': []
                   }

        for task in job["JobSummary"]["Tasks"]:
            for task_drop_item in TASK_DROP_ITEMS:
                task.pop(task_drop_item, None)
                LOGGER.debug('task pop: <{0}>'.format(pformat(task_drop_item)))

            # fix kafka job return float
            for item in NEED_INT:
                if item in task['Config']:
                    task['Config'][item] = int(task['Config'][item])
                    LOGGER.debug('task int: <{0}>=<{1}>'.format(pformat(item), task['Config'][item]))
            if 'ConnectionConfig' in task['Config'] and 'Port' in task['Config']['ConnectionConfig']:
                task['Config']['ConnectionConfig']['Port'] = int(task['Config']['ConnectionConfig']['Port'])
                LOGGER.debug('task int: <{0}>=<{1}>'.format('Port', task['Config']['ConnectionConfig']['Port']))

            if task['Type'].lower() == 'src':
                # fix RuntimeError: dictionary changed size during iteration
                temp_task_config = deepcopy(task['Config'])
                for k, _ in temp_task_config.items():
                    if k not in NOMAD_SRC_CFG_WHITE_LIST:
                        task['Config'].pop(k)
                        LOGGER.debug('src pop: <{0}>'.format(pformat(k)))

                if 'ReplicateDoDb' in task['Config']:
                    for db in task['Config']['ReplicateDoDb']:
                        if 'TableSchemaRename' in db and '$' in db['TableSchemaRename']:
                            db['TableSchemaRename'] = db['TableSchemaRename'].replace('$', '$$')
                        if 'Tables' in db:
                            for table in db['Tables']:
                                if 'TableRename' in table and '$' in table['TableRename']:
                                    table['TableRename'] = table['TableRename'].replace('$', '$$')

            elif task['Type'].lower() == 'dest':
                # fix RuntimeError: dictionary changed size during iteration
                temp_task_config = deepcopy(task['Config'])
                for k, _ in temp_task_config.items():
                    if k not in NOMAD_DEST_CFG_WHITE_LIST:
                        task['Config'].pop(k)
                        LOGGER.debug('dest pop: <{0}>'.format(pformat(k)))
            else:
                LOGGER.warning('unknown task type: <{0}>'.format(pformat(task['Type'])))

            new_job['Tasks'].append(task)

        LOGGER.debug('now_job: <{0}>'.format(pformat(new_job)))
        with open(job['Name'] + ".json", "w") as f:
            json.dump(new_job, f, indent=2)

def  dtleHelp():
    print("Usage:  [help] [args]")
    print("args：")
    print("DTLE_IP      --old dtle ip,not null")
    print("DTLE_PORT    --old dtle port,not null")
def main(argv):
    init_log()
    LOGGER.info('*' + 'FROM DTLE TO NOMAD START'.center(28) + '*')
    if len(argv) == 2 and (argv[1]=="help"or argv[1]=="args"):
        dtleHelp()
    else if len(argv) == 3:
        DTLE_IP = argv[1]
        DTLE_PORT = argv[2]
        jobs = get_jobs(DTLE_IP , DTLE_PORT)
        parse(jobs)
        print('Please replace database password by yourself!')
        LOGGER.info('*' + 'FROM DTLE TO NOMAD EMD'.center(28) + '*')
    else:
        print("agrv number err, Usage:  [help] [args] ")
        print("args：")
        print("DTLE_IP      --old dtle ip,not null")
        print("DTLE_PORT    --old dtle port,not null")
main(sys.argv)
