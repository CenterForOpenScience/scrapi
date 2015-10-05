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
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties

logger = logging.getLogger(__name__)


def process_contributors(authors):
    ret = []
    aux = []
    authors_list = authors.split('|')
    for author in authors_list:
        author_list = author.split(';')
        if author_list[0] in aux:
            continue
        else:
            contributor = {}
            aux.append(author_list[0])
            contributor['sameAs'] = [author_list[0]]
            contributor['name'] = author_list[1]
            ret.append(contributor)
    return ret


def process_publisher(publisher):
    ret = {'name': publisher}
    return ret


def process_result(result):
    processed_result = {}
    for field in result:
        processed_result[field] = result[field]['value']
    return processed_result


def process_object_uris(object_uris):
    processed_uris = []
    object_uris_array = object_uris.split('|')
    if object_uris_array[0]:
        doi_uri = 'https://dx.doi.org/' + object_uris_array[0]
        processed_uris.append(doi_uri)
    if object_uris_array[1]:
        pmid_uri = 'http://www.ncbi.nlm.nih.gov/pubmed/' + object_uris_array[1]
        processed_uris.append(pmid_uri)
    return processed_uris


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

    record_encoding = None

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

    def sparql_query(self, start_date, end_date, limit, offset):
        query_str = """ PREFIX vivo:  <http://vivoweb.org/ontology/core#>
                        PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
                        PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
                        PREFIX bibo:  <http://purl.org/ontology/bibo/>
                        PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
                        PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>

                        SELECT ?URI ?title ?abstract ?journalTitle ?date ?publisher ?startPage ?endPage ?doi (CONCAT(?doi, "|", ?PMID) as ?objectUris) ?volume ?number ?ISSN ?ISBN
                                (group_concat(distinct ?type ; separator = "|") AS ?types)
                                (group_concat(distinct ?subject ; separator = "|") AS ?subjects)
                                (group_concat(distinct ?keyword ; separator = "|") AS ?keywords)
                                (group_concat(distinct ?author ; separator = "|") AS ?authors)
                        {{
                            ?URI vivo:dateTimeValue ?dateURI .
                            ?dateURI vivo:dateTime ?date .
                            OPTIONAL {{
                                        ?URI <http://purl.org/dc/terms/title> ?title .
                                    }}
                            OPTIONAL {{
                                        ?journal vivo:publicationVenueFor ?URI .
                                        ?journal rdfs:label ?journalTitle .
                                    }}
                            OPTIONAL {{
                                        ?journal vivo:publicationVenueFor ?URI .
                                        ?journal bibo:issn ?ISSN .
                                    }}
                            OPTIONAL {{
                                        ?journal vivo:publicationVenueFor ?URI .
                                        ?journal bibo:isbn ?ISBN .
                                    }}
                            OPTIONAL {{
                                        ?publisherURI vivo:publisherOf ?journal .
                                        ?publisherURI rdfs:label ?publisher .
                                     }}
                            OPTIONAL {{
                                        ?subjectURI vivo:subjectAreaOf ?URI .
                                        ?subjectURI rdfs:label ?subject .
                                    }}
                            OPTIONAL {{
                                        ?URI vivo:freetextKeyword ?keyword .
                                    }}
                            OPTIONAL {{
                                        ?URI vitro:mostSpecificType ?typeURI .
                                        ?typeURI rdfs:label ?type .
                                    }}
                            OPTIONAL {{
                                        ?URI bibo:abstract ?abstract .
                                    }}
                            OPTIONAL {{
                                        ?URI bibo:doi ?doi .
                                    }}
                            OPTIONAL {{
                                        ?URI bibo:volume ?volume .
                                    }}
                            OPTIONAL {{
                                        ?URI bibo:number ?number .
                                    }}
                            OPTIONAL {{
                                        ?URI bibo:pageStart ?startPage .
                                    }}
                            OPTIONAL {{
                                        ?URI bibo:pageEnd ?endPage .
                                    }}
                            OPTIONAL {{
                                        ?URI bibo:pmid ?PMID .
                                   }}
                            OPTIONAL {{
                                        ?authorship a vivo:Authorship .
                                        ?authorship vivo:relates ?URI .
                                        ?authorURI vivo:relatedBy ?authorship .
                                        ?authorURI a foaf:Person .
                                        ?authorURI rdfs:label ?authorName .
                                        BIND ( CONCAT(str(?authorURI), ";" , ?authorName) AS ?author) .
                                    }}
                            FILTER (strdt(?date, xsd:date) >= "{}"^^xsd:date && strdt(?date, xsd:date) <= "{}"^^xsd:date)
                        }} GROUP BY ?URI ?doi ?abstract ?title ?journalTitle ?date ?publisher ?volume ?number ?startPage ?endPage ?PMID ?ISSN ?ISBN LIMIT {} OFFSET {}
                    """
        query_str = query_str.format(start_date.isoformat(), end_date.isoformat(), limit, offset)
        self.sparql_wrapper.setQuery(query_str)
        results = self.sparql_wrapper.query()
        results = results.convert()
        return results['results']['bindings']

    @property
    def schema(self):
        return {
            'title': ('/title', lambda x: x if x else ''),
            'description': ('/subtitle', lambda x: x[0] if (isinstance(x, list) and x) else x or ''),
            'providerUpdatedDateTime': ('/date', lambda x: datetime.strptime(x, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%S%Z") + '+00:00'),
            'uris': {
                'canonicalUri': '/URI',
                'providerUris': ['/URI'],
                'objectUris': ('/objectUris', lambda x: process_object_uris(x) if x else [])
            },
            'contributors': ('/authors', lambda x: process_contributors(x) if x else []),
            'subjects': ('/subjects', lambda x: x.split('|') if x else []),
            'tags': ('/keywords', lambda x: x.split('|') if x else []),
            'publisher': ('/publisher', lambda x: process_publisher(x) if x else {}),
            'otherProperties': build_properties(
                ('journalTitle', '/journalTitle'),
                ('abstract', ('/abstract', lambda x: x if x else '')),
                ('issue', '/issue'),
                ('types', ('/types', lambda x: x.split('|') if x else [])),
                ('ISSN', ('/ISSN', lambda x: x if x else '')),
                ('number', '/number'),
                ('ISBN', '/ISBN'),
                ('startPage', '/startPage'),
                ('endPage', '/endPage'),
                ('issue', '/issue'),
                ('volume', '/volume'),
            )
        }

    def harvest(self, start_date=None, end_date=None):
        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        total = self.get_total(start_date, end_date)
        logger.info('{} documents to be harvested'.format(total))

        doc_list = []
        for i in xrange(0, total, 1000):
            records = self.sparql_query(start_date, end_date, 1000, i)
            logger.info('Harvested {} documents'.format(i + len(records)))

            for record in records:
                if 'doi' in record:
                    doc_id = record['doi']['value']
                else:
                    doc_id = record['URI']['value']
                doc_list.append(RawDocument({
                    'doc': json.dumps(process_result(record)),
                    'source': self.short_name,
                    'docID': doc_id,
                    'filetype': 'json'
                }))

        return doc_list
