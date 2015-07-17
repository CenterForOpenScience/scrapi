from django.test import TestCase
from rest_framework.test import APIRequestFactory

from webview.views import DocumentList


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
