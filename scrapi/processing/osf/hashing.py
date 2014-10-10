# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unicodedata
import string
import hashlib


def get_id(doc):
    return normalize_string(doc['id']['serviceID'])


def get_source(doc):
    return normalize_string(doc['source'])


def get_doi(doc):
    return normalize_string(doc['id']['doi'] + get_title(doc))


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
    astring = astring.replace(u'—', u'')
    bstring = unicodedata.normalize('NFKD', astring).encode('ascii','ignore')
    exclude = set(string.punctuation)
    exclude.add(' ')
    exclude.add('\n')
    bstring = ''.join(ch for ch in bstring if ch not in exclude)
    return bstring  # returns the essence of the string, as a string


def get_contributors(doc):
    contributors = doc['contributors'] # this is a list
    namelist = ''
    for contributor in contributors:
        fullname = contributor['given'] + contributor['family']
        namelist += fullname
    namelist = sorted(namelist) # alphabetical order, in case contrib order varies by source
    namelist = ''.join(namelist)
    namelist = normalize_string(namelist)
    namelist = hashlib.md5(namelist).hexdigest() # should be shorter as md5 than full list
    return normalize_string(namelist) # returns a list of strings


def get_title(doc):
    title = doc['title']
    title = normalize_string(title)
    title = hashlib.md5(title).hexdigest()  # should be shorter on average than full title
    return title


def is_project(doc):
    return ';isProject:true'


REPORT_HASH_FUNCTIONS = [get_title, get_contributors, get_doi, get_id]
RESOURCE_HASH_FUNCTIONS = [get_title, get_contributors]
