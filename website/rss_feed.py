import search
import PyRSS2Gen as pyrss
import local as settings
import datetime
import logging
from cStringIO import StringIO
logger = logging.getLogger(__name__)


@search.requires_search
def gen_rss_feed(raw_query):
    results, count = search.search('scrapi', raw_query, start=0, size=100)
    logger.info('{n} results returned from search'.format(n=len(results)))
    xml = dict_to_rss(results, count, raw_query)
    return xml


def dict_to_rss(results, count, query):
    if not query:
        query = "everything"
    docs = results

    items = [
        pyrss.RSSItem(
            title=str(doc.get('title')),
            link='http://' + settings.URL + '/' + doc.get('location')[0],
            description=format_description(doc),
            guid=str(doc.get('id')),
            pubDate=str(doc.get('timestamp'))
        ) for doc in docs if doc.get('location') is not None
    ]
    logger.info("{n} documents added to RSS feed".format(n=len(items)))
    rss = pyrss.RSS2(
        title='scrAPI: RSS feed for documents retrieved from query: "{query}"'.format(query=query),
        link='{base_url}/rss?q={query}'.format(base_url=settings.URL, query=query),
        items=items,
        description='{n} results, {m} most recent displayed in feed'.format(n=count, m=len(items)),
        lastBuildDate=str(datetime.datetime.now()),
    )

    f = StringIO()
    rss.write_xml(f)

    return f.getvalue()


def format_description(doc):
    contributors = doc.get('contributors')
    contributors = '; '.join([contributor['full_name'] for contributor in doc.get('contributors')])
    timestamp = 'Retrieved from {source} at {timestamp}<br/>'.format(source=doc.get('source'), timestamp=doc.get('timestamp'))
    return '<br/>'.join([str(contributors), timestamp, str(_get_description(doc))])


def _get_description(doc):
    properties = doc.get('properties') if isinstance(doc, dict) else None
    result = None
    if properties:
        abstract = properties.get('abstract')
        description = properties.get('description')
        result = abstract if abstract else description
    return result if result else "Description not provided"
