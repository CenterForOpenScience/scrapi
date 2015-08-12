import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import django
from django.test import TestCase
from rest_framework.test import APIRequestFactory

from api.webview.views import DocumentList

django.setup()


class APIViewTests(TestCase):

    def setUp(self):
        self.factory = APIRequestFactory()

    def test_document_view(self):
        view = DocumentList.as_view()
        request = self.factory.get(
            '/documents/'
        )
        response = view(request)

        self.assertEqual(response.status_code, 200)

    def test_source_view(self):
        view = DocumentList.as_view()
        request = self.factory.get(
            '/documents/dudley_weekly/'
        )
        response = view(request)

        self.assertEqual(response.status_code, 200)

    def test_individual_view(self):
        view = DocumentList.as_view()
        request = self.factory.get(
            '/documents/dudley_weekly/dudley1'
        )
        response = view(request)

        self.assertEqual(response.status_code, 200)
