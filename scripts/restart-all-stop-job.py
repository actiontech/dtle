# coding=UTF-8
#!/usr/bin/python
import json
import requests
# change the host to your host
host="10.186.62.104:8190"
r = requests.get('http://'+host+'/v1/jobs')
jobs = r.json()
print(r.status_code)
#data2 = json.loads(jobs)
for value in jobs:
  if value["Status"]!="running":
    res = requests.get('http://'+host+'/v1/job/'+value["ID"]+'/resume')
    #if res.status_code==200

  
