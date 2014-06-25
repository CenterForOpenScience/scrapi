__author__ = 'faye'
import requests

myReq = requests.get('http://api.plos.org/search?q=abstract:%22why%22&api_key=7AbMQ-D-3hKy5hJ1FPY2')

fileName = str(articleID)+".html"
f = open("plos_articles/"+fileName, 'w')
f.write(myReq.text.encode('utf-8'))
f.close()
