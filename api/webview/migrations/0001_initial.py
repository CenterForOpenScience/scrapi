# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django_pgjson.fields


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Document',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('source', models.CharField(max_length=255)),
                ('docID', models.TextField()),
                ('providerUpdatedDateTime', models.DateTimeField(null=True)),
                ('raw', django_pgjson.fields.JsonField()),
                ('normalized', django_pgjson.fields.JsonField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='HarvesterResponse',
            fields=[
                ('key', models.TextField(serialize=False, primary_key=True)),
                ('method', models.CharField(max_length=8)),
                ('url', models.TextField()),
                ('ok', models.NullBooleanField()),
                ('content', models.BinaryField(null=True)),
                ('encoding', models.TextField(null=True)),
                ('headers_str', models.TextField(null=True)),
                ('status_code', models.IntegerField(null=True)),
                ('time_made', models.DateTimeField(auto_now=True)),
            ],
        ),
    ]
