'''
Harvester for the PDXScholar for the SHARE project

Example API call: http://pdxscholar.library.pdx.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class PdxscholarHarvester(OAIHarvester):
    short_name = 'pdxscholar'
    long_name = 'PDXScholar Portland State University'
    url = 'http://pdxscholar.library.pdx.edu'

    base_url = 'http://pdxscholar.library.pdx.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'setSpec']
    timezone_granularity = True

    approved_sets = [u'actg', u'actg_fac', u'anth_fac', u'anthos', u'anthos_archives',
                     u'ling_fac', u'bio_fac', u'eng_bookpubpaper', u'busadmin_fac',
                     u'childfamily_pub', u'centerforlakes_pub', u'clee_pubs', u'publicservice_pub',
                     u'realestate_pub', u'scienceeducation_fac', u'chem_fac', u'chla_fac',
                     u'cengin_fac', u'cengin_gradprojects', u'cengin_honorstheses', u'comm_fac',
                     u'commhealth_fac', u'compsci_fac', u'coun_fac', u'ccj_fac', u'ccj_capstone',
                     u'ci_fac', u'etds', u'open_access_etds', u'econ_fac', u'edu_fac', u'elp_fac',
                     u'ece_fac', u'etm_fac', u'esm_fac', u'geog_fac', u'geog_masterpapers',
                     u'geology_fac', u'hist_fac', u'naturalresources_pub', u'iss_pub',
                     u'metropolitianstudies', u'aging_pub', u'is_fac', u'lse_comp', u'ulib_fac',
                     u'lltr', u'mem_gradprojects', u'usp_murp', u'mth_fac', u'mengin_fac',
                     u'metroscape', u'pdxopen', u'mcnair', u'phl_fac', u'phy_fac', u'prc',
                     u'psy_fac', u'pubadmin_fac', u'realestate_workshop', u'usp_planning',
                     u'rri_facpubs', u'socwork_fac', u'soc_fac', u'sped_fac', u'sphr_fac',
                     u'sysc_fac', u'honorstheses', u'usp_fac', u'wgss_fac', u'wll_fac',
                     u'younghistorians']
