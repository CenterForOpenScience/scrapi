
# SciTech Connect is a portal to free, publicly-available DOE-sponsored R&D results including 
# technical reports, bibliographic citations, journal articles, conference papers, books, multimedia 
# and data information. SciTech Connect is a consolidation of two core DOE search engines, the 
# Information Bridge and the Energy Citations Database. SciTech Connect incorporates all of the 
# R&D information from these two products into one search interface. SciTech Connect was 
# developed by the U.S. Department of Energy (DOE) Office of Scientific and Technical 
# Information (OSTI) to increase access to science, technology, and engineering research 
# information from DOE and its predecessor agencies.
# 
# Lots more info at:
# https://www.osti.gov/home/sites/www.osti.gov.home/files/SciTechXMLDataServices.pdf
# 
# Here's a python script to gather some data using their API

# goal of our wrapper...

# date range
# return full text

# SciTech.daterange()

# SciTech.id()

import requests
from lxml import etree
import datetime

class ScitechData(object):

    def __init__(self):
        self.base_url = 'http://www.osti.gov/scitech/scitechxml?'

    def date(self, startdate):
        ''' Gets articles in a given date '''

        startdate = 'EntryDateFrom=' + startdate

        result = requests.get(self.base_url + startdate)

        root = etree.fromstring(result.content)

        num_results = root[0].get("count")

        print num_results


thang = ScitechData()

thang.date("12/13/13")



# request_type = 'page='
# page_number = 1
# request = requests.get(base_url + request_type + str(page_number))

# root = etree.fromstring(request.content)

# num_results = root[0].get("count")
# num_results

# root[0].get("morepages")

# todays_date = datetime.date.today().strftime('%m/%d/%Y')
# todays_date

# # <codecell>

# request_type = 'EntryDateFrom='
# recent_updated_url = base_url + request_type + todays_date

# # <codecell>

# recent_updated_url

# # <codecell>


