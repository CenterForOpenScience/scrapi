# Create your models here.
import collections

from django.db import models

from jsonfield import JSONField


class Document(models.Model):
    source = models.CharField(max_length=100)
    docID = models.CharField(max_length=100)

    providerUpdatedDateTime = models.DateTimeField(null=True)

    raw = JSONField(load_kwargs={'object_pairs_hook': collections.OrderedDict})
    normalized = JSONField(load_kwargs={'object_pairs_hook': collections.OrderedDict}, null=True)
