"""
A VIVO harvester for the SHARE project

This harvester makes a SPARQL query to the VIVO SPARQL endpoint,
the information to acces to a VIVO endpoint must be provided in the local.py file.
"""

from __future__ import unicode_literals

import json
import logging

from datetime import date, timedelta, datetime

from six.moves import xrange
from SPARQLWrapper import SPARQLWrapper, JSON

from scrapi import settings
from scrapi.settings.sparql_mapping import *
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties

logger = logging.getLogger(__name__)


def process_object_uris(pmid, doi):
    ret = []
    if pmid:
        pmid = 'http://www.ncbi.nlm.nih.gov/pubmed/{}'.format(pmid)
        ret.append(pmid)
    if doi:
        doi = 'https://dx.doi.org/{}'.format(doi)
        ret.append(doi)
    return ret


class VIVOHarvester(JSONHarvester):
    short_name = 'vivo'
    long_name = 'VIVO'
    url = settings.VIVO_ACCESS['url']
    base_url = settings.VIVO_ACCESS['query_endpoint']
    sparql_wrapper = SPARQLWrapper(base_url)
    sparql_wrapper.setReturnFormat(JSON)
    sparql_wrapper.addParameter("email", settings.VIVO_ACCESS['username'])
    sparql_wrapper.addParameter("password", settings.VIVO_ACCESS['password'])
    sparql_wrapper.method = 'GET'

    DEFAULT_ENCODING = 'UTF-8'
    QUERY_TEMPLATE = """
                       PREFIX vivo:  <http://vivoweb.org/ontology/core#>
                       PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
                       PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
                       PREFIX bibo:  <http://purl.org/ontology/bibo/>
                       PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
                       PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>
                       PREFIX dc: <http://purl.org/dc/terms/>
                       PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
                       PREFIX obo:   <http://purl.obolibrary.org/obo/>

                       SELECT {}
                       {{
                            {}
                        }}
                     """

    record_encoding = None

    def get_string(self, uri, sparql_map):
        variable = sparql_map['name']
        pattern = sparql_map['pattern'].format(uri)
        sparql_query = self.QUERY_TEMPLATE.format('?' + variable, pattern)
        self.sparql_wrapper.setQuery(sparql_query)
        result = self.sparql_wrapper.query()
        result = result.convert()
        if len(result['results']['bindings']) > 0:
            return result['results']['bindings'][0][variable]['value']
        else:
            return ''

    def get_array(self, uri, sparql_map):
        variable = sparql_map['fields'][0]
        pattern = sparql_map['pattern'].format(uri)
        sparql_query = self.QUERY_TEMPLATE.format('?' + variable, pattern)
        self.sparql_wrapper.setQuery(sparql_query)
        results = self.sparql_wrapper.query()
        results = results.convert()
        ret = []
        for result in results['results']['bindings']:
            ret.append(result[variable]['value'])
        return ret

    def get_dict(self, uri, sparql_map):
        variables = ''
        for variable in sparql_map['fields']:
            variables += '?' + variable + ' '
        pattern = sparql_map['pattern'].format(uri)
        sparql_query = self.QUERY_TEMPLATE.format(variables, pattern)
        self.sparql_wrapper.setQuery(sparql_query)
        results = self.sparql_wrapper.query()
        results = results.convert()
        ret = []
        for result in results['results']['bindings']:
            item = {}
            for variable in sparql_map['fields']:
                item[variable] = result[variable]['value']
            ret.append(item)
        return ret

    def get_records(self, uris):
        records = []
        for uri in uris:
            record = {}
            record['uri'] = uri
            for sparql_map in SPARQL_MAPPING:
                if sparql_map['type'] == 'string':
                    record[sparql_map['name']] = self.get_string(uri, sparql_map)
                if sparql_map['type'] == 'array':
                    record[sparql_map['name']] = self.get_array(uri, sparql_map)
                if sparql_map['type'] == 'dict':
                    record[sparql_map['name']] = self.get_dict(uri, sparql_map)
            records.append(record)
        return records

    def get_total(self, start_date, end_date):
        query_str = """PREFIX vivo:  <http://vivoweb.org/ontology/core#>
                       PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
                       SELECT (COUNT(*) AS ?total)
                       {{
                            ?s vivo:dateTimeValue ?dateURI .
                            ?dateURI vivo:dateTime ?date .
                            FILTER (strdt(?date, xsd:date) >= "{}"^^xsd:date && strdt(?date, xsd:date) <= "{}"^^xsd:date)
                        }}"""
        query_str = query_str.format(start_date.isoformat(), end_date.isoformat())
        self.sparql_wrapper.setQuery(query_str)
        result = self.sparql_wrapper.query()
        result = result.convert()
        return int(result['results']['bindings'][0]['total']['value'])

    def get_uris(self, start_date, end_date, limit, offset):
        uris = []
        query_str = """ PREFIX vivo:  <http://vivoweb.org/ontology/core#>
                        PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>

                        SELECT ?uri
                        {{
                            ?uri vivo:dateTimeValue ?dateUri .
                            ?dateUri vivo:dateTime ?date .
                            FILTER (strdt(?date, xsd:date) >= "{}"^^xsd:date && strdt(?date, xsd:date) <= "{}"^^xsd:date)
                        }}  LIMIT {} OFFSET {}
                    """
        query_str = query_str.format(start_date.isoformat(), end_date.isoformat(), limit, offset)
        self.sparql_wrapper.setQuery(query_str)
        results = self.sparql_wrapper.query()
        results = results.convert()
        for result in results['results']['bindings']:
            uris.append(result['uri']['value'])
        return uris

    def complete_authors(self, authors):
        for author in authors:
            pattern = """ <{}> obo:ARG_2000028 ?vcard .
                          ?vcard vcard:hasEmail ?emailUri .
                          ?emailUri vcard:email ?email ."""
            email = self.get_string(author['sameAs'], {'name': 'email', 'pattern': pattern, 'type': 'string'})
            if email:
                author['email'] = email

            pattern = """?role obo:RO_0000052 <{}> .
                         ?role vivo:roleContributesTo ?organization .
                         ?organization rdfs:label ?name .
                         ?role vivo:dateTimeInterval ?interval .
                        FILTER NOT EXISTS {{ ?interval vivo:end ?end }}"""
            affiliation = self.get_dict(author['sameAs'], {'name': 'affiliation', 'fields': ['name'], 'pattern': pattern, 'type': 'dict'})
            if affiliation:
                author['affiliation'] = affiliation

            pattern = """<{}> vivo:orcidId ?orcidId .
                        FILTER isURI(?orcidId)"""
            orcidId = self.get_string(author['sameAs'], {'name': 'orcidId', 'pattern': pattern, 'type': 'string'})
            author['sameAs'] = [author['sameAs']]
            if orcidId:
                author['sameAs'].append(orcidId)
        return authors

    @property
    def schema(self):
        return {
            'title': ('/title', lambda x: x if x else ''),
            'providerUpdatedDateTime': ('/date', lambda x: datetime.strptime(x, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%S%Z") + '+00:00'),
            'uris': {
                'canonicalUri': '/uri',
                'providerUris': ['/uri'],
                'objectUris': ('/pmid', '/doi', lambda x, y: process_object_uris(x, y))
            },
            'contributors': ('/authors', lambda x: self.complete_authors(x)),
            'subjects': '/subjects',
            'tags': '/keywords',
            'publisher': ('/publisher', lambda x: {'name': x} if x else ''),
            'otherProperties': build_properties(
                ('journalTitle', '/journalTitle'),
                ('abstract', ('/abstract', lambda x: x if x else '')),
                ('type', '/types'),
                ('ISSN', ('/issn', lambda x: x if x else '')),
                ('number', '/number'),
                ('ISBN', '/isbn'),
                ('startPage', '/startPage'),
                ('endPage', '/endPage'),
                ('volume', '/volume'),
            )
        }

    def harvest(self, start_date=None, end_date=None):
        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        total = self.get_total(start_date, end_date)
        logger.info(' {} documents to be harvested'.format(total))

        doc_list = []
        for i in xrange(0, total, 1000):
            uris = self.get_uris(start_date, end_date, 1000, i)
            records = self.get_records(uris)
            logger.info('Harvested {} documents'.format(i + len(records)))

            for record in records:
                if 'doi' in record:
                    doc_id = record['doi']
                else:
                    doc_id = record['uri']
                doc_list.append(RawDocument({
                    'doc': json.dumps(record),
                    'source': self.short_name,
                    'docID': doc_id,
                    'filetype': 'json'
                }))

        return doc_list
