from django.conf.urls import url
from webview import views

urlpatterns = [
    url(r'^documents/$', views.DocumentList.as_view()),
    url(r'^view_records/$', views.view_records, name='view_records'),
]
