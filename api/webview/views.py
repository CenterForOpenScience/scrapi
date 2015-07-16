import json
from xml.dom import minidom
from xml.parsers.expat import ExpatError

from django.http import Http404
from django.shortcuts import render
from rest_framework import generics
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.views.decorators.clickjacking import xframe_options_exempt

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


@api_view(['GET'])
@xframe_options_exempt
def document_detail(request, docID):
    """
    Retrieve, update or delete a document instance.
    """
    try:
        document = Document.objects.get(docID=docID)
    except document.DoesNotExist:
        return Response(status=Http404)

    if request.method == 'GET':
        serializer = DocumentSerializer(document)
        return Response(serializer.data)
