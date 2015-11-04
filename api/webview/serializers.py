from rest_framework import serializers

from api.webview.models import Document


class DocumentSerializer(serializers.ModelSerializer):

    class Meta:
        model = Document
        fields = ('key', 'providerUpdatedDateTime', 'source', 'docID', 'raw', 'normalized')
