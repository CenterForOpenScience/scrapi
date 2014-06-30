import scitech_scraper as SciTechScraper
from lxml import etree
import datetime

param_date_stamp = datetime.date.today().strftime('%m/%d/%Y')

print SciTechScraper.consume()
print SciTechScraper.parse()
SciTechScraper.xml_to_text()










#root = etree.fromstring(TestScrape.xml.encode('utf-8'))
#for x in root.find('records'):
#	for y in x:
#		print y


