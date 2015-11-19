from django.db import models
from django_pgjson.fields import JsonField


class Document(models.Model):
    key = models.TextField(primary_key=True)
    source = models.CharField(max_length=255)
    docID = models.TextField()

    providerUpdatedDateTime = models.DateTimeField(null=True)

    raw = JsonField()
    timestamps = JsonField(null=True)
    normalized = JsonField(null=True)
    status = models.TextField(null=True)

    def save(self, *args, **kwargs):
        if not self.key:
            self.key = self._make_key(self.source, self.docID)
        return super(Document, self).save(*args, **kwargs)

    @classmethod
    def _make_key(cls, source, docID):
        return '|'.join((source, docID))


class Version(models.Model):
    key = models.ForeignKey(Document)

    source = models.CharField(max_length=255)
    docID = models.TextField()
    timestamps = JsonField()

    providerUpdatedDateTime = models.DateTimeField(null=True)

    raw = JsonField()
    normalized = JsonField(null=True)
    status = models.TextField(null=True)


class HarvesterResponse(models.Model):

    key = models.TextField(primary_key=True)

    method = models.CharField(max_length=8)
    url = models.TextField()

    # Raw request data
    ok = models.NullBooleanField(null=True)
    content = models.BinaryField(null=True)
    encoding = models.TextField(null=True)
    headers_str = models.TextField(null=True)
    status_code = models.IntegerField(null=True)
    time_made = models.DateTimeField(auto_now=True)
