"""
Harvests metadata from the Digital Commons at Wayne State for the SHARE project

More information at https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.wayne.md

Example API call: http://digitalcommons.wayne.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2014-09-29T00:00:00Z
"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class WayneStateHarvester(OAIHarvester):
    short_name = 'waynestate'
    long_name = 'Digital Commons @ Wayne State'
    url = 'http://digitalcommons.wayne.edu'

    base_url = 'http://digitalcommons.wayne.edu/do/oai/'
    property_list = [
        'type', 'source', 'publisher', 'format',
        'date', 'setSpec', 'identifier'
    ]
    approved_sets = [
        'acb_frp',
        'agtc',
        'anthrofrp',
        'bio_fuel',
        'biomed_eng_frp',
        'biomedcentral',
        'biosci_frp',
        'business_frp',
        'ce_eng_frp',
        'chemfrp',
        'cjfrp',
        'cmmg',
        'coe_aos',
        'coe_khs',
        'coe_tbf',
        'coe_ted',
        'commfrp',
        'commsci_frp',
        'compscifrp',
        'cpcs_pubs',
        'csdt',
        'ec_eng_frp',
        'englishfrp',
        'geofrp',
        'gerontology',
        'humbiol_preprints',
        'iehs',
        'im_eng_frp',
        'immunology_frp',
        'libsp',
        'mathfrp',
        'med_anesthesiology',
        'med_biochem',
        'med_cardio',
        'med_cher',
        'med_dermatology',
        'med_dho',
        'med_did',
        'med_dpacs',
        'med_edm',
        'med_em',
        'med_intmed',
        'med_neurology',
        'med_neurosurgery',
        'med_obgyn',
        'med_ohn_surgery',
        'med_oncology',
        'med_opthalmology',
        'med_ortho_surgery',
        'med_path',
        'med_pbn',
        'med_pediatrics',
        'med_pmr',
        'med_radiology',
        'med_ro',
        'med_surgery',
        'med_urology',
        'mott_pubs',
        'musicfrp',
        'nfsfrp',
        'nursingfrp',
        'pet',
        'pharm_appsci',
        'pharm_healthcare',
        'pharm_practice',
        'pharm_science',
        'pharma_frp',
        'philofrp',
        'phy_astro_frp',
        'physio_frp',
        'prb',
        'provost_pub',
        'psychfrp',
        'skillman',
        'slisfrp',
        'soc_work_pubs',
        'socfrp',
        'urbstud_frp',
        'antipodes',
        'criticism',
        'discourse',
        'framework',
        'humbiol',
        'jewishfilm',
        'jmasm',
        'marvels',
        'mpq',
        'narrative',
        'storytelling'
    ]
