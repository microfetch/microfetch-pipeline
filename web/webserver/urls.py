import django.http
from django.urls import path
from . import views


urlpatterns = [
    path('healthcheck/', views.healthcheck),
    path('callback/', views.callback, name='callback'),
    path('post/', views.post),
    path('', views.index, name='index'),
]
