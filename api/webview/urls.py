from django.conf.urls import url
from api.webview import views

urlpatterns = [
    url(r'^documents/$', views.DocumentList.as_view()),
    url(r'^documents/(?P<source>\w+)/$', views.DocumentsFromSource.as_view(), name='source'),
    url(r'^documents/(?P<source>[a-z]+)/(?P<docID>(.*))/$', views.document_detail, name='document_detail')
]
