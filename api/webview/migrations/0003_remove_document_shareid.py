# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('webview', '0002_auto_20150709_1129'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='document',
            name='shareID',
        ),
    ]
