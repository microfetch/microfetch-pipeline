from django.urls import path
from . import views


urlpatterns = [
    path('callback/', views.callback, name='callback'),
    path('', views.index, name='index'),
]