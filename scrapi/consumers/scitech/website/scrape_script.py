import consume
import parse
from flask import Flask, request, Response
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

app = Flask(__name__)
app.debug = True

@app.route('/consume', methods=['GET', 'POST'])
def consume_day():
	return Response(consume.consume())

@app.route('/process', methods=['GET', 'POST'])
def parse_all():
	result = request.args.get('doc')
	print result
	print request.args
	timestamp = request.args.get('timestamp')
	return Response(parse.parse(result, timestamp))

if __name__ == '__main__':
	app.run(
		host="0.0.0.0",
		port=1338,
		debug=True
	)











#root = etree.fromstring(TestScrape.xml.encode('utf-8'))
#for x in root.find('records'):
#	for y in x:
#		print y


