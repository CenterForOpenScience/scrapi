def hash_id(doc):
    return doc['id']['serviceID'].lower().strip()


def hash_title(doc):
    return doc['title'].lower().strip()

REPORT_HASH_FUNCTIONS = [hash_id, hash_title]
RESOURCE_HASH_FUNCTIONS = [hash_title, hash_id]
