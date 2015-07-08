import collections

from rest_framework import serializers

from jsonfield import JSONField

from scrapi.processing.postgres import Document


class DocumentSerializer(serializers.Serializer):
    shareID = serializers.IntegerField(read_only=True)

    source = serializers.CharField(max_length=100)
    docID = serializers.CharField(max_length=100)

    providerUpdatedDateTime = serializers.DateTimeField()

    raw = JSONField(load_kwargs={'object_pairs_hook': collections.OrderedDict})
    normalized = JSONField(load_kwargs={'object_pairs_hook': collections.OrderedDict})

    def create(self, validated_data):
        """
        Create and return a new `Document` instance, given the validated data.
        """
        return Document.objects.create(**validated_data)

    def update(self, instance, validated_data):
        """
        Update and return an existing `Snippet` instance, given the validated data.
        """

        instance.source = validated_data.get('source', instance.source)
        instance.docID = validated_data.get('docID', instance.docID)
        instance.providerUpdatedDateTime = validated_data.get('providerUpdatedDateTime', instance.providerUpdatedDateTime)
        instance.raw = validated_data.get('raw', instance.raw)
        instance.normalized = validated_data.get('normalized', instance.normalized)

        instance.save()
        return instance
