# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django_pgjson.fields


class Migration(migrations.Migration):

    dependencies = [
        ('webview', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Version',
            fields=[
                ('id', models.AutoField(primary_key=True, auto_created=True, verbose_name='ID', serialize=False)),
                ('source', models.CharField(max_length=255)),
                ('docID', models.TextField()),
                ('timestamps', django_pgjson.fields.JsonField()),
                ('providerUpdatedDateTime', models.DateTimeField(null=True)),
                ('raw', django_pgjson.fields.JsonField()),
                ('normalized', django_pgjson.fields.JsonField(null=True)),
            ],
        ),
        migrations.AddField(
            model_name='document',
            name='status',
            field=models.TextField(null=True),
        ),
        migrations.AddField(
            model_name='document',
            name='timestamps',
            field=django_pgjson.fields.JsonField(null=True),
        ),
        migrations.AddField(
            model_name='version',
            name='key',
            field=models.ForeignKey(to='webview.Document'),
        ),
    ]
