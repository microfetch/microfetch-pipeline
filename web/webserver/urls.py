from django.urls import path
from . import views


urlpatterns = [
    path('healthcheck/', views.healthcheck),
    path('post/', views.post),
    path('', views.index, name='index'),
    path('api/taxons/', views.api_taxons, name='taxons'),
    path('api/taxon/<str:taxon_id>/', views.api_taxon, name='taxon'),
    path('api/accession/<str:accession_id>/', views.api_accession, name='accession'),
    path('api/request_assembly_candidate/', views.api_request_assembly_candidate, name='assembly_request'),
    path(
        'api/confirm_assembly_candidate/<str:accession_id>/',
        views.api_confirm_assembly_candidate,
        name='assembly_confirm'
    )
]
