from rest_framework import generics
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.views.decorators.clickjacking import xframe_options_exempt

from api.webview.models import Document
from api.webview.serializers import DocumentSerializer


class DocumentList(generics.ListCreateAPIView):
    """
    List all documents in the SHARE API
    """
    serializer_class = DocumentSerializer

    def perform_create(self, serializer):
        serializer.save(source=self.request.user)

    def get_queryset(self):
        """ Return all documents
        """
        queryset = Document.objects.all()

        return queryset


class DocumentsFromSource(generics.ListCreateAPIView):
    """
    List all documents from a particular source
    """
    serializer_class = DocumentSerializer

    def perform_create(self, serializer):
        serializer.save(source=self.request.user)

    def get_queryset(self):
        """ Return queryset based on source
        """
        queryset = Document.objects.filter(source=self.kwargs['source'])

        return queryset


@api_view(['GET'])
@xframe_options_exempt
def document_detail(request, source, docID):
    """
    Retrieve one particular document.
    """
    try:
        all_sources = Document.objects.filter(source=source)
        document = all_sources.get(docID=docID)
    except Document.DoesNotExist:
        return Response(status=404)

    if request.method == 'GET':
        serializer = DocumentSerializer(document)
        return Response(serializer.data)
