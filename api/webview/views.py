from django.shortcuts import render
from rest_framework import generics

from webview.models import Document
from webview.serializers import DocumentSerializer

from xml.dom import minidom
from xml.parsers.expat import ExpatError


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

    queryset = Document.objects.all()
    document_list = []

    for doc in queryset:
        the_d = {}
        try:
            this_xml = minidom.parseString(doc.raw['doc'].encode('utf-8'))
        except ExpatError:
            this_xml = minidom.parseString('<xml></xml>')
        the_d['raw'] = (this_xml.toprettyxml())
        the_d['normed'] = doc.normalized
        document_list.append(the_d)

    return render(
        request,
        'webview/view_records.html',
        {'document_list': document_list}
    )
