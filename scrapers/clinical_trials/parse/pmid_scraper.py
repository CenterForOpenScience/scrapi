from bs4 import BeautifulSoup
import requests

def get_pubmed_info(pmid):
    BASE = 'http://www.ncbi.nlm.nih.gov/pubmed/'
    pmid_info = {}

    html = requests.get(BASE + pmid)
    soup = BeautifulSoup(html.text)

    abstract_div = soup.find("div", {"class": "rprt abstract"})

    pmid_info['title'] = abstract_div.find("h1").text
    pmid_info['authors'] = abstract_div.find("div", {"class":"auths"}).text
    pmid_info['abstract'] =  abstract_div.find("div", {"class":"abstr"}).text

    return pmid_info


# ideas for saving the files: 
# for file in xml:
#     if the file has requests in it:
#         get the pmid

#         use the pmid to scrape the site
#         save a line of a CSV file with info from the site
#         each line: nct_id, title, authors, abstract


# when importing the CSV file...
# for each line of the 