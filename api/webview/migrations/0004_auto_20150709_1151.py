# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import jsonfield.fields


class Migration(migrations.Migration):

    dependencies = [
        ('webview', '0003_remove_document_shareid'),
    ]

    operations = [
        migrations.AlterField(
            model_name='document',
            name='normalized',
            field=jsonfield.fields.JSONField(blank=True),
        ),
        migrations.AlterField(
            model_name='document',
            name='providerUpdatedDateTime',
            field=models.DateTimeField(blank=True),
        ),
    ]
