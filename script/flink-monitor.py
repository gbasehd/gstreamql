import sys
import json
import requests
from urlparse import urljoin

#BASE_URL = 'http://192.167.1.222:8081'
BASE_URL = sys.argv[3]
RUNNING = '/joboverview/running'
JOBOVERVIEW = '/joboverview'
AUTH = ('admin', 'admin')
ERROR = 'StreamQL Error : '

def main():
    print(globals()[sys.argv[1]](sys.argv[2]))

def getResponse(req):
    rsp = requests.get(urljoin(BASE_URL, req), auth=AUTH, headers={'Accept': 'application/json'})
    return rsp.json()

def getRunningJobId(jobName):
    try:
        result = getResponse(RUNNING)
    except Exception, e:
        return ERROR + str(e)
    jobId = None 
    for k in result:
        for i in result[k]:
	    if i['name']==jobName:
	        jobId = i['jid']
    return jobId

def getStatusByJobId(jobId):
    try:
        result = getResponse(JOBOVERVIEW)
    except Exception, e:
        return ERROR + str(e)
    jobStatus = None
    for k in result:
        for i in result[k]:
            if i['jid'] == jobId:
                jobStatus = i['state'] 
    return jobStatus

if __name__ == "__main__":
    main()
