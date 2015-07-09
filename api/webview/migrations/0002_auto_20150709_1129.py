# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('webview', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='document',
            name='docID',
            field=models.CharField(max_length=100),
        ),
    ]
