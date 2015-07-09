from django.conf.urls import url
from webview import views

urlpatterns = [
    url(r'^documents/$', views.DocumentList.as_view())
]
