from glob import glob
from lxml import etree
import xmltodict
import time
from Bio import Entrez
from Bio import Medline
from bs4 import BeautifulSoup
import requests



def get_locations(root):
    """After opening an XML file with etree.parse(file_name).getroot(), the root is passed to this function 
    and the location info is extracted from the XML."""    
    locations = []
    for entry in root.findall('location'):
        location_dict = {'name': None, 'zip': None, 'city': None, 'state': None, 'country': None}
        if entry.find('facility').find('name') != None:
            location_dict['name'] = entry.find('facility').find('name').text
        if entry.find('facility').find('address').find('zip') != None:
            location_dict['zip'] = entry.find('facility').find('address').find('zip').text
        if entry.find('facility').find('address').find('city') != None:
            location_dict['city'] = entry.find('facility').find('address').find('city').text
        if entry.find('facility').find('address').find('country') != None:
            location_dict['country'] = entry.find('facility').find('address').find('country').text
        if entry.find('facility').find('address').find('state') != None:
            location_dict['state'] = entry.find('facility').find('address').find('state').text
        locations.append(location_dict)
    return locations
 
def parse(file_name):
    """Used to extract information from an XML document from clinicaltrials.gov and to return Json.
    (which is essentially the same format as a python dictionary.)"""
    trial = {}
    root = etree.parse(file_name).getroot()
    trial['id'] = root.find('id_info').find('nct_id').text
    # # test!!
    # print "trial IS is: " + trial['id']

    trial['phase'] = root.find('phase').text
    trial['description'] = root.find('brief_summary')[0].text
   
    if root.find('official_title') == None:
        trial['title'] = root.find('brief_title').text
    else:
        trial['title'] = root.find('official_title').text

    trial['contributors'] = []
    for entry in root.findall('overall_official'):
        official_dict = {}
        official_dict['full_name'] = entry.find('last_name').text
        official_dict['role'] = entry.find('role').text
        official_dict['affiliation'] = entry.find('affiliation').text
        official_dict['email'] = None
        trial['contributors'].append(official_dict)

    trial['keywords'] = ["clinical trial"]
    for entry in root.findall('keyword'):
        trial['keywords'].append(entry.text)

    date_string = root.find('required_header').find('download_date').text
    trial['date_processed'] = date_string.replace('ClinicalTrials.gov processed this data on ', '')

    trial['locations'] = get_locations(root)

    trial['references'] = []
    for entry in root.findall('reference'):
        reference_dict = {}
        reference_dict['citation'] = entry.find('citation').text
        if entry.find('PMID') != None:
            reference_dict['PMID'] = entry.find('PMID').text
            reference_dict['article_info'] = get_pubmed_info(reference_dict['PMID'])
        trial['references'].append(reference_dict)
    return trial
    

def xml_to_json(xml_file):
    """Converts xml to Json and returns a tuple containing the the Json object created from the XML 
    and location data returned from processing the XML using get_locations."""
    tree = etree.parse(xml_file)
    xml_string = etree.tostring(tree)
    blob = xmltodict.parse(xml_string)
    locations = get_locations(tree.getroot())
    return blob, locations

def scrape_pubmed_info(pmid):
    """Scrapes pumbed, SHOULD NOT BE USED."""
    BASE = 'http://www.ncbi.nlm.nih.gov/pubmed/'
    pmid_info = {}

    html = requests.get(BASE + pmid)
    soup = BeautifulSoup(html.text)

    abstract_div = soup.find("div", {"class": "rprt abstract"})

    pmid_info['title'] = abstract_div.find("h1").text
    pmid_info['authors'] = abstract_div.find("div", {"class":"auths"}).text
    pmid_info['abstract'] =  abstract_div.find("div", {"class":"abstr"}).text
    pmid_info['link'] = BASE + pmid

    return pmid_info

def get_pubmed_info(pmid):
    """Retrieves article info from pubmed using Biopython's Entrez module. Need to insert a real email for the "Entrez.email" variable below."""
    pmid_info = {}
    BASE = 'http://www.ncbi.nlm.nih.gov/pubmed/'
    Entrez.email = 'insert_email_here@email.com'
    handle = Entrez.efetch(db="pubmed", id=pmid, rettype="medline", retmode="text")
    records = Medline.parse(handle)
    for record in records:
        if record.get("id:"):
            pmid_info['info'] = 'Outdated PMID: No article information could be retrieved at this time.'
            continue
        pmid_info['title'] = record.get("TI", "?")
        pmid_info['authors'] = record.get("AU", "?")
        pmid_info['abstract'] =  record.get("AB", "?")
        pmid_info['link'] = BASE + pmid
    return pmid_info

def add_pubmed_to_references(nct_json):
    """Adds additional pubmed info to the references section of the version Json."""
    refs = None
    bg = nct_json.get('clinical_study').get('background')
    if bg: 
        refs = bg.get('reference')
    if not bg:
        bg = nct_json.get('clinical_study').get('results')
        if bg:
            refs = bg.get('reference')
    if refs:
        if not isinstance(refs, list):
            refs = [refs]
        for element in refs:
            PMID = element.get('medline_ui')
            if PMID:
                info  = get_pubmed_info(PMID)
                element['article_info'] = info



def json_osf_format(nct_id):
    """Takes a clinicaltrials.gov nct_id and returns json in a format containing info about the trial in a format
    that can be imported by the OSF."""
    files = set([f.rstrip('-before').rstrip('-after') for f in glob('files/{0}/*.xml'.format(nct_id))])
    files = sorted(files, key=lambda v: time.mktime(time.strptime(v.split('/')[-1].rstrip('.xml').split('_')[-1], '%Y%m%d')))


    if len(files) == 0:
        return None
    
    trial = parse('xml/{0}.xml'.format(nct_id))

    versions = {}
    for f in files:
        version = f.split('/')[-1].rstrip('.xml').split('_')[-1]
        v, locations = xml_to_json(f)
#        v['clinical_study']['location'] = l2c(locations)
#       v['clinical_study']['references'] = add_pubmed_to_references(v)
        v['clinical_study']['keyword'] = ([v['clinical_study'].get('keyword')] or [])+trial['keywords']
#        add_pubmed_to_references(v)
        versions[version] = v

    json_osf = {        
        "imported_from": "clinicaltrials.gov",
        "date_processed": trial['date_processed'],
        "contributors": trial['contributors'],
        "description": trial['description'],
        "id": trial['id'],
        "title": trial['title'],
        "files": glob('files/{0}/*'.format(str(trial['id']))),
        "tags": ["clinicaltrial.gov"]+(trial.get('keywords') or []),
        "versions": versions,
        "keywords": trial['keywords'],
        "references": trial['references'],
        "source": "clinical_trials"
    }
        
    return json_osf


