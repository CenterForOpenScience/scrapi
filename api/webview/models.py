from django.db import models

from django_pgjson.fields import JsonField


class Document(models.Model):
    source = models.CharField(max_length=255)
    docID = models.TextField()

    providerUpdatedDateTime = models.DateTimeField(null=True)

    raw = JsonField()
    normalized = JsonField()
