# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import jsonfield.fields


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Document',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('source', models.CharField(max_length=100)),
                ('docID', models.IntegerField()),
                ('shareID', models.IntegerField()),
                ('providerUpdatedDateTime', models.DateTimeField()),
                ('raw', jsonfield.fields.JSONField()),
                ('normalized', jsonfield.fields.JSONField()),
            ],
        ),
    ]
