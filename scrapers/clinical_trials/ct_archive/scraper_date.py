__author__ = 'faye'
import requests
import sys
import os

reload(sys)
sys.setdefaultencoding('utf-8')

def getIDs():
    allIds = []
    fileIds = open('../key_ids.txt', 'r')

    for line in fileIds:
        lineId = line.split()
        lineClean = lineId[0].translate(None, "[',u")
        allIds.append(lineClean)

    return allIds

for id in getIDs():
    myReq = requests.get('http://clinicaltrials.gov/archive/'+id)

    if not os.path.exists("ct_dates/"+id):
        os.makedirs("ct_dates/"+id)

    f = open("ct_dates/"+id+"/"+id+".html", 'w')
    f.write(myReq.text)
    f.close()
    print id+" dates collected."