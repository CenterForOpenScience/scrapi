# trying to match, whether or not from the same source

def get_id(doc):
    return doc['id']['serviceID'].lower().strip()


def get_source(doc):
    return doc['source']


def get_url(doc):
    return doc['id']['url']


def get_doi(doc):
    return doc['id']['doi'] + get_title(doc)


def normalize_string(astring): # helper function for grab_ funcs; takes a unicode string
    astring = astring.lower()
    # stop words - happy to add more
    stopwords = ['the', 'a', 'an', 'about', 'do', 'does', 'what', 'who', 'it', 'to', 'has', 'had', 'in', 'by']
    for word in stopwords:
        word = ' ' + word + ' '
        astring = astring.replace(word, ' ')
    # docs.python.org/2/library/unicodedata.html
    # TODO: this may not work for some unicode characters; I dealt w/ known special cases
    # (when it fails to transliterate, it replaces with '')
    astring = astring.replace(u'æ', u'ae')
    astring = astring.replace(u'Æ', u'Ae')
    astring = astring.replace(u'ß', u'ss') # assumes good transliteration
    bstring = unicodedata.normalize('NFKD', astring).encode('ascii','ignore')
    exclude = set(string.punctuation)
    exclude.add(' ')
    exclude.add('\n')
    bstring = ''.join(ch for ch in bstring if ch not in exclude)
    #bstring = hashlib.md5(bstring).hexdigest()
    return bstring  # returns the essence of the string, as a string


def get_contributors(doc):
    contributors = doc['contributors'] # this is a list
    namelist = ''
    for contributor in contributors:
        fullname = contributor['given'] + contributor['family']
        namelist += fullname
    return normalize_string(namelist) # returns a list of strings


def get_title(doc):
    title = doc['title']
    return normalize_string(title)  # returns a unicode string


def is_project(doc):
    return 'cebwrpg'



REPORT_HASH_FUNCTIONS = [get_id, get_title, get_contributors, get_doi]
RESOURCE_HASH_FUNCTIONS = [get_title, get_contributors, get_doi, is_project]
