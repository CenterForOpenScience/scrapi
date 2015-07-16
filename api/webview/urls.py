from django.conf.urls import url
from webview import views

urlpatterns = [
    url(r'^documents/$', views.DocumentList.as_view()),
    url(r'^documents/(.*)/$', views.document_detail, name='document_detail')
]
