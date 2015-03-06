from copy import deepcopy


def default_name_parser(names):
    return names


def update_schema(old, new):
    d = deepcopy(old)
    for key, value in new.items():
        d[key] = value
    return d


BASEXMLSCHEMA = {
    "description": '//dc:description/node()',
    "contributors": ['//dc:creator/node()', default_name_parser],
    "title": '//dc:title/node()',
    "dateUpdated": '//dc:dateEntry/node()',
    "id": {
        "url": '//dcq:identifier-citation/node()',
        "serviceID": '//dc:ostiId/node()',
        "doi": '//dc:doi/node()'
    },
    "tags": '//dc:subject/node()'
}
