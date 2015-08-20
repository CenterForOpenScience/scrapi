from django.db import models
from django_pgjson.fields import JsonField


class Document(models.Model):
    source = models.CharField(max_length=100)
    docID = models.CharField(max_length=100)

    providerUpdatedDateTime = models.DateTimeField(null=True)

    raw = JsonField()
    normalized = JsonField()


class HarvesterResponse(models.Model):

    method = models.CharField(max_length=8)
    url = models.TextField()

    # Raw request data
    ok = models.BooleanField()
    content = models.BinaryField()
    encoding = models.TextField()
    headers_str = models.TextField()
    status_code = models.IntegerField()
    time_made = models.DateTimeField(auto_now=True)
