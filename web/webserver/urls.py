from django.views.generic import TemplateView
from rest_framework.schemas import get_schema_view
from django.urls import path
from . import views


urlpatterns = [
    path('healthcheck/', views.healthcheck),
    path('swagger-ui/', TemplateView.as_view(
        template_name='swagger-ui.html',
        extra_context={'schema_url': 'openapi-schema'}
    ), name='swagger-ui'),
    path('openapi/', get_schema_view(
        title="ENA Monitor",
        description="API for tracking and assembling new genomic data in the European Nucleotide Archive.",
        version="0.1.0"
    ), name='openapi-schema'),

    path('post/', views.post),
    path('', views.index, name='index'),

    path('api/taxons/', views.ListTaxons.as_view(), name='taxons'),
    path('api/taxon/<str:taxon_id>/', views.ViewTaxon.as_view(), name='taxon'),
    path('api/record/<str:record_id>/', views.ViewRecord.as_view(), name='record'),
    path('api/request_assembly_candidate/', views.RequestAssemblyCandidate.as_view(), name='assembly_request'),
    path(
        'api/confirm_assembly_candidate/<str:record_id>/',
        views.AcceptAssemblyCandidate.as_view(),
        name='assembly_confirm'
    ),
    path('api/qualifyr_report_fields/', views.QualifyrReportFields.as_view(), name='qualifyr_report_fields')
]
