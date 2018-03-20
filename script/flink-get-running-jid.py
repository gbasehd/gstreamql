import json
import sys
import requests
from urlparse import urljoin

BASE_URL = 'http://192.167.1.225:8081'
AUTH = ('admin', 'admin')

def main():
    print(getRunningJobId(sys.argv[1]))

def getRunningJobId(jobName):

    rsp = requests.get(urljoin(BASE_URL, '/joboverview/running'), auth=AUTH, headers={'Accept': 'application/json'})
    result = rsp.json()

    jobId = None
    for k in result:
        for i in result[k]:
            if i['name']==jobName:
                jobId = i['jid']

    return jobId

if __name__ == "__main__":
    main()
