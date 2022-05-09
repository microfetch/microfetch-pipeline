from django.urls import path
from . import views


urlpatterns = [
    path('callback/', views.callback, name='callback'),
    path('post/', views.post),
    path('', views.index, name='index'),
]