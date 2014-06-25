from lxml import etree
import json
import xmltodict
from glob import glob
from os import mkdir

dirs = glob('ct_changes_xml/*')
for d in dirs:
    folder = d.split('/')[-1]
    mkdir('ct_changes_json/{0}'.format(folder))
    xmlfiles = glob('{0}/*.xml'.format(d))    
    for xml in xmlfiles:
        fname = '/'.join(xml.split('/')[1:]).rstrip('.xml')
        f = open('ct_changes_json/{0}.json'.format(fname), 'wb')
        tree = None
        try:
            tree = etree.parse(xml)
        except:
            print xml
        if tree:     
            xml_string = etree.tostring(tree)
            json_text = xmltodict.parse(xml_string)
            json_text = json.dumps(json_text, sort_keys=True, indent=4)
            f.write(json_text)
            f.close() 


