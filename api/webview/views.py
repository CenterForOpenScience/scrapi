import json
from django.http import HttpResponse
from rest_framework import generics
from rest_framework import permissions
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.views.decorators.clickjacking import xframe_options_exempt

from api.webview.models import Document
from api.webview.serializers import DocumentSerializer


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
