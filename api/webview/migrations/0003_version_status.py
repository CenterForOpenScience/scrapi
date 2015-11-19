# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('webview', '0002_auto_20151113_1515'),
    ]

    operations = [
        migrations.AddField(
            model_name='version',
            name='status',
            field=models.TextField(null=True),
        ),
    ]
