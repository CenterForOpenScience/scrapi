"""
A harvester for City Universtiy of New York for the SHARE project

An example API call: http://academicworks.cuny.edu/do/oai/request?
verb=ListRecords&metadataPrefix=oai_dc
"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester

class CUNY_Harvester(OAIHarvester):
	short_name = 'cuny'
	long_name = 'City University of New York'
	url = 'http://academicworks.cuny.edu'

	base_url = 'http://academicworks.cuny.edu/do/oai/'
	property_list = ['publisher', 'language', 'format', 'source', 'date', 'identifier', 'type']

	approved_Sets = [u'gc_etds', u'gc_etds_legacy', u'bx_conf_bet14', u'ho_conf_bet15', 
		u'ols_proceedings_lac', u'gj_etds', u'gc_cs_tr', u'ufs_conf', u'ols_proceedings', 
		u'gc_etds_all', u'gc_econ_wp', u'cl_pubs', u'gc_pubs', u'ufs_conf_sp15', 
		u'gc_studentpubs', u'cc_conf_hic', u'lacuny_conf_2015_gallery', u'lacuny_conf_2015',
		 u'cc_etds_theses', u'lg_oers', u'bc_oers', u'cc_oers', u'oers', u'ny_oers', 
		 u'qc_oers', u'gc_oers', u'hc_oers', u'si_oers', u'qb_oers', u'nc_oers', u'bx_oers', 
		 u'ny_pubs', u'jj_pubs', u'yc_pubs', u'sph_pubs', u'pubs', u'bc_pubs', u'bx_pubs', 
		 u'nc_pubs', u'bb_pubs', u'qc_pubs', u'oaa_pubs', u'gj_pubs', u'kb_pubs', u'lg_pubs', 
		 u'si_pubs', u'cc_pubs', u'ho_pubs', u'qb_pubs', u'le_pubs', u'bm_pubs', u'dsi_pubs', 
		 u'me_pubs', u'hc_pubs', u'hc_sas_etds', u'bb_etds', u'etds', u'ufs'
	]