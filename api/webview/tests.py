import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import pytest
import django
from django.test import TestCase
from rest_framework.test import APIRequestFactory

from api.webview.views import DocumentList

django.setup()


# TODO - make this work without Django.

class APIViewTests(TestCase):

    def setUp(self):
        self.factory = APIRequestFactory()

    @pytest.mark.postgres
    def test_document_view(self):
        view = DocumentList.as_view()
        request = self.factory.get(
            '/documents/'
        )
        response = view(request)

        self.assertEqual(response.status_code, 200)
