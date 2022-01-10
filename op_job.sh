#!/bin/bash
# operate oracle job
get_token(){
    token=$(curl -X POST "http://10.186.61.75:8190/v2/loginWithoutVerifyCode" -H "accept: application/json" -H "Content-Type: application/json" -d "{ \"password\": \"VnOcOaPLW3v03fxVMkNmd9nWF/m2GhO6yHz9cZ3udljVOwqkXTskXKYlwqoeETrGf2fnpaa6kD+xefyzq5//lqk6ymO/4CjNxQTWGY9s41ngRhM+7GZtNZaYbvl4DM46/C7i2iwsVPySeKpvZLCH3c36HYzUP23xlWClnLYGKuA=\", \"tenant\": \"platform\", \"username\": \"admin\"}" | python -c "import sys, json; print json.load(sys.stdin)['data']['token']")
    echo "$token"
}


create_job(){
    op_file=$op_obj
    if [ -z "$op_file" ]
    then
        op_file="job.json"
    fi
    job_json=$(cat ./$op_file)
    create_result=$(curl -X POST "http://10.186.61.75:8190/v2/job/sync/create" -H "accept: application/json" -H "Authorization: $token" -H "Content-Type: application/json" -d "$job_json")
    echo "create result $create_result"
}

delete_job(){
    job_id=$op_obj
    if [ -z "$job_id" ]
    then
        job_id="oracle-sync"
    fi
    create_result=$(curl -X POST "http://10.186.61.75:8190/v2/job/sync/delete" -H "accept: application/json" -H "Authorization: $token" -H "Content-Type: application/x-www-form-urlencoded" -d "job_id=$job_id")
    echo "delete result $create_result"
}

op_type=$1
op_obj=$2
echo $op_type,$op_obj
# get token
get_token
# operate job
if [ "$op_type" == "d" ]
then
    delete_job
else
    create_job
fi
