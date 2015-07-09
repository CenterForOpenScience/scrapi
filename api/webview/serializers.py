from rest_framework import serializers

from scrapi.processing.postgres import Document


class DocumentSerializer(serializers.ModelSerializer):

    class Meta:
        model = Document
        fields = ('id', 'providerUpdatedDateTime', 'source', 'docID', 'raw', 'normalized')
