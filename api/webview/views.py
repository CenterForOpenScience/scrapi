import json
from xml.dom import minidom
from xml.parsers.expat import ExpatError

from django.shortcuts import render
from rest_framework import generics

from webview.models import Document
from webview.serializers import DocumentSerializer


class DocumentList(generics.ListCreateAPIView):
    """
    List all pushed data, or push to the API
    """
    serializer_class = DocumentSerializer

    def perform_create(self, serializer):
        serializer.save(source=self.request.user)

    def get_queryset(self):
        """ Return queryset based on from and to kwargs
        """
        queryset = Document.objects.all()

        return queryset


def view_records(request):

    source_list = [entry['source'] for entry in Document.objects.order_by().values('source').distinct()]
    source_list.append('All')

    queryset = Document.objects.all()
    document_list = []

    for doc in queryset:
        the_d = {}
        try:
            this_xml = minidom.parseString(doc.raw['doc'].encode('utf-8'))
        except ExpatError:
            this_xml = minidom.parseString('<xml></xml>')
        the_d['raw'] = (this_xml.toprettyxml())
        the_d['normed'] = json.dumps(doc.normalized, indent=4)
        document_list.append(the_d)

    return render(
        request,
        'webview/view_records.html',
        {'document_list': document_list, 'sources': source_list}
    )
