# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import jsonfield.fields


class Migration(migrations.Migration):

    dependencies = [
        ('webview', '0004_auto_20150709_1151'),
    ]

    operations = [
        migrations.AlterField(
            model_name='document',
            name='normalized',
            field=jsonfield.fields.JSONField(null=True),
        ),
        migrations.AlterField(
            model_name='document',
            name='providerUpdatedDateTime',
            field=models.DateTimeField(null=True),
        ),
    ]
