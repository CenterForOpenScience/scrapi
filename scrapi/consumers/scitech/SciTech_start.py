# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <rawcell>

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

# <codecell>

import requests
from lxml import etree
import datetime

# <codecell>

base_url = 'http://www.osti.gov/scitech/scitechxml?'

# <markdowncell>

# To get data about the entire dataset...

# <codecell>

request_type = 'page='
page_number = 1

# <codecell>

request = requests.get(base_url + request_type + str(page_number))

# <codecell>

root = etree.fromstring(request.content)

# <markdowncell>

# To see the number of results in the entire data set...

# <codecell>

num_results = root[0].get("count")
num_results

# <markdowncell>

# To check if there are pages beyond the one you're viewing...

# <codecell>

root[0].get("morepages")

# <markdowncell>

# To check to see what items have been updated since yesterday...

# <codecell>

todays_date = datetime.date.today().strftime('%m/%d/%Y')
todays_date

# <codecell>

request_type = 'EntryDateFrom='
recent_updated_url = base_url + request_type + todays_date

# <codecell>

recent_updated_url

# <codecell>


