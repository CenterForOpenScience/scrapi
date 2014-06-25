__author__ = 'faye'
from bs4 import BeautifulSoup
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

def getDates(soup):
    dates = []
    for date in soup.findAll('', {'class': 'si-date'}):
        dates.append(str(date.string))
    dates.pop(0)
    return dates
