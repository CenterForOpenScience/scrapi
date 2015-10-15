DOCUMENT_MAPPING = [
    {
        'name': 'title',
        'pattern': '<{}> dc:title|rdfs:label ?title .',
        'type': 'string'
    },
    {
        'name': 'journalTitle',
        'pattern': """?journal vivo:publicationVenueFor <{}> ;
                                 rdfs:label ?journalTitle .""",
        'type': 'string'
    },
    {
        'name': 'doi',
        'pattern': '<{}> bibo:doi ?doi .',
        'type': 'string'
    },
    {
        'name': 'issn',
        'pattern': """?journal vivo:publicationVenueFor <{}> ;
                               bibo:issn ?issn .""",
        'type': 'string'
    },
    {
        'name': 'isbn',
        'pattern': """?journal vivo:publicationVenueFor <{}> ;
                               bibo:isbn ?isbn .""",
        'type': 'string'
    },
    {
        'name': 'date',
        'pattern': """<{}>  vivo:dateTimeValue ?dateURI .
                      ?dateURI vivo:dateTime ?date .""",
        'type': 'string'
    },
    {
        'name': 'volume',
        'pattern': '<{}> bibo:volume ?volume .',
        'type': 'string'
    },
    {
        'name': 'number',
        'pattern': '<{}> bibo:number ?number .',
        'type': 'string'
    },
    {
        'name': 'startPage',
        'pattern': '<{}> bibo:pageStart ?startPage .',
        'type': 'string'
    },
    {
        'name': 'endPage',
        'pattern': '<{}> bibo:pageEnd ?endPage .',
        'type': 'string'
    },
    {
        'name': 'pmid',
        'pattern': '<{}> bibo:pmid ?pmid .',
        'type': 'string'
    },
    {
        'name': 'publisher',
        'pattern': """?journal vivo:publicationVenueFor <{}> .
                      ?publisherURI vivo:publisherOf ?journal ;
                                    rdfs:label ?publisher .""",
        'type': 'string'
    },
    {
        'name': 'subjects',
        'fields': ['subject'],
        'pattern': """?subjectURI vivo:subjectAreaOf <{}> ;
                                  rdfs:label ?subject .""",
        'type': 'array'
    },
    {
        'name': 'abstract',
        'pattern': '<{}> bibo:abstract ?abstract .',
        'type': 'string'
    },
    {
        'name': 'keywords',
        'fields': ['keyword'],
        'pattern': '<{}> vivo:freetextKeyword ?keyword .',
        'type': 'array'
    },
    {
        'name': 'types',
        'fields': ['type'],
        'pattern': """<{}> vitro:mostSpecificType ?typeURI .
                      ?typeURI rdfs:label ?type .""",
        'type': 'array'
    },
    {
        'name': 'authors',
        'fields': ['sameAs', 'name', 'givenName', 'familyName'],
        'pattern': """?authorship a vivo:Authorship .
                      ?authorship vivo:relates <{}> .
                      ?sameAs vivo:relatedBy ?authorship .
                      ?sameAs a foaf:Person .
                      ?sameAs rdfs:label ?name .
                      ?sameAs obo:ARG_2000028 ?vcard .
                      ?vcard vcard:hasName ?nameUri .
                      ?nameUri vcard:familyName ?familyName .
                      ?nameUri vcard:givenName ?givenName .""",
        'type': 'dict'
    }
]

AUTHOR_MAPPING = {
    'email': {
        'name': 'email',
        'pattern': """<{}> obo:ARG_2000028 ?vcard .
                      ?vcard vcard:hasEmail ?emailUri .
                      ?emailUri vcard:email ?email .""",
        'type': 'string'
    },
    'affiliation': {
        'name': 'affiliation',
        'fields': ['name'],
        'pattern': """?role obo:RO_0000052 <{}> .
                      ?role vivo:roleContributesTo ?organization .
                      ?organization rdfs:label ?name .
                      ?role vivo:dateTimeInterval ?interval .
                      FILTER NOT EXISTS {{ ?interval vivo:end ?end }}""",
        'type': 'dict'
    },
    'orcidId': {
        'name': 'orcidId',
        'pattern': """<{}> vivo:orcidId ?orcidId .
                      FILTER isURI(?orcidId)""",
        'type': 'string'
    }
}
