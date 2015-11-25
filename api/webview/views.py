import json
from django.http import HttpResponse
from rest_framework import generics
from rest_framework import permissions
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.views.decorators.clickjacking import xframe_options_exempt

from elasticsearch import Elasticsearch

from scrapi import settings
from api.webview.models import Document
from api.webview.serializers import DocumentSerializer

es = Elasticsearch(settings.ELASTIC_URI, request_timeout=settings.ELASTIC_TIMEOUT)


class DocumentList(generics.ListAPIView):
    """
    List all documents in the SHARE API
    """
    serializer_class = DocumentSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def perform_create(self, serializer):
        serializer.save(source=self.request.user)

    def get_queryset(self):
        """ Return all documents
        """
        return Document.objects.all()


class DocumentsFromSource(generics.ListAPIView):
    """
    List all documents from a particular source
    """
    serializer_class = DocumentSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def perform_create(self, serializer):
        serializer.save(source=self.request.user)

    def get_queryset(self):
        """ Return queryset based on source
        """
        return Document.objects.filter(source=self.kwargs['source'])


@api_view(['GET'])
@xframe_options_exempt
def document_detail(request, source, docID):
    """
    Retrieve one particular document.
    """
    try:
        document = Document.objects.get(key=Document._make_key(source, docID))
    except Document.DoesNotExist:
        return Response(status=404)

    serializer = DocumentSerializer(document)
    return Response(serializer.data)


@api_view(['GET'])
@xframe_options_exempt
def status(request):
    """
    Show the status of the API
    """
    return HttpResponse(json.dumps({'status': 'ok'}), content_type='application/json', status=200)


@api_view(['GET', 'POST'])
def institutions(request):
    if not es:
        return HttpResponse('No connection to elastic search', status=503)
    if request.data.get('query') or request.query_params.get('q'):
        query = request.data.get('query') or {
            'query': {
                'query_string': {
                    'query': request.query_params.get('q')
                }
            }
        }
    else:
        query = {
            'query': {
                'match_all': {}
            }
        }

    es.indices.create(index='institutions', ignore=400)
    res = es.search(index=settings.ELASTIC_INST_INDEX, body=query)
    # validate query and grab whats wanted
    try:
        res = {
            'results': [val['_source'] for val in res['hits']['hits']],
            'aggregations': res.get('aggregations') or res.get('aggs'),
            'count': res['hits']['total']
        }
    except IndexError:
        return Response('Invalid query', status=400)
    return Response(res, status=200)
