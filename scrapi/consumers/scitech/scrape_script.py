from scitech_scraper import SciTechScraper
from lxml import etree


TestScrape = SciTechScraper()
TestScrape.xml_to_text()

print TestScrape.parse()[5]
print TestScrape.url



#root = etree.fromstring(TestScrape.xml.encode('utf-8'))
#for x in root.find('records'):
#	for y in x:
#		print y


