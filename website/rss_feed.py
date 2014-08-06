import search
import PyRSS2Gen as pyrss
import local as settings
import datetime
import logging
from cStringIO import StringIO
logger = logging.getLogger(__name__)


@search.requires_search
def gen_rss_feed(raw_query):
    results = search.search('scrapi', raw_query, start=0, size=100)
    xml = dict_to_rss(results, raw_query)
    return xml


def dict_to_rss(results, query):
    if not query:
        query = "everything"
    docs = results

    items = [
        pyrss.RSSItem(
            title=str(doc.get('title')),
            link='http://' + settings.URL + '/' + doc.get('location')[0],
            description=str(_get_description(doc)),
            guid=str(doc.get('id')),
            pubDate=str(doc.get('timestamp'))
        ) for doc in docs if doc.get('location') is not None
    ]
    logger.info("{n} documents added to RSS feed".format(n=len(items)))

    rss = pyrss.RSS2(
        title='RSS feed for documents retrieved from query: "{query}"'.format(query=query),
        link='{base_url}/rss?q={query}'.format(base_url=settings.URL, query=query),
        items=items,
        description='Exactly what it sounds like',
        lastBuildDate=str(datetime.datetime.now()),
    )

    f = StringIO()
    rss.write_xml(f)

    return f.getvalue()


def _get_description(doc):
    properties = doc.get('properties') if isinstance(doc, dict) else None
    result = None
    if properties:
        abstract = properties.get('abstract')
        description = properties.get('description')
        result = abstract if abstract else description
    return result if result else "Description not provided"
