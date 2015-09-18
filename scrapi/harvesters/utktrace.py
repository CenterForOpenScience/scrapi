'''
Harvester for the Trace: Tennessee Research and Creative Exchange for the SHARE project
Example API call: http://trace.tennessee.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UtkTraceHarvester(OAIHarvester):
    short_name = 'utktrace'
    long_name = 'Trace: Tennessee Research and Creative Exchange'
    url = 'http://trace.tennessee.edu'

    base_url = 'http://trace.tennessee.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True

    approved_sets = [u'utk_accopubs', u'utk_agrieconpubs', u'utk_animpubs', u'utk_anthpubs',
                     u'utk_architecpubs', u'utk_audipubs', u'utk_aviapubs', u'utk_biocpubs', u'utk_compmedpubs',
                     u'utk_biospubs', u'utk_botapubs', u'utk_statpubs', u'utk_cssjpapers', u'catalyst',
                     u'utk_chembiopubs', u'utk_chempubs', u'utk_chilpubs', u'utk_civipubs', u'utk_claspubs',
                     u'utk_lawpubl', u'utk-lawjournals', u'utk_compmatdata', u'utk_datasets', u'utk_davisbacon',
                     u'utk_rotcarmypubs', u'utk_biolpubs', u'utk_eartpubs', u'utk_ecolpubs', u'utk_econpubs',
                     u'utk_edleadpubs', u'utk_educpubs', u'utk_elecutsipubs', u'utk_englpubs', u'utk_entopubs',
                     u'utk_compmedpeer', u'utk_smalpeer', u'utk_largpeer', u'utk_famipubs', u'utk_foodpubs',
                     u'utk_forepubs', u'gamut', u'utk_geno', u'utk_indupubs', u'utk_induengipubs', u'utk_nuclearpubs',
                     u'utk_instpubs', u'utk_intepubs', u'ijns', u'utk_exerpubs', u'utk_largpubs', u'utk_latipubs',
                     u'utk_mtaspubs', u'utk_manapubs', u'utk_markpubs', u'utk_matepubs', u'utk_mathutsipubs',
                     u'utk_mechutsipubs', u'utk_micrpubs', u'utk_modepubs', u'utk_molecularsimulation',
                     u'utk_nuclpubs', u'utk_libfac', u'utk_artfac', u'utk_artpeer', u'utk_libpub',
                     u'utk_physastrpubs', u'utk_psycpubs', u'utk_bakecentpubs', u'pursuit', u'utk_relipubs',
                     u'utk_infosciepubs', u'utk_jourpubs', u'utk_socipubs', u'utk_socopubs', u'rgsj', u'jaepl',
                     u'utk_theapubs', u'utk_theopubs', u'utgradmedpubs', u'vernacular']
