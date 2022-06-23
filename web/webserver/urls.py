from django.views.generic import TemplateView
from rest_framework.schemas import get_schema_view
from django.urls import path
from rest_framework.routers import DefaultRouter
from . import views

import logging

logger = logging.getLogger(__file__)

router = DefaultRouter()
router.register(r'records', views.RecordViewSet, basename='record')
router.register(r'taxons', views.TaxonViewSet, basename='taxon')

urlpatterns = [
    *router.urls,
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

    path('qualifyr_report_fields/', views.QualifyrReportFields.as_view(), name='qualifyr_report_fields')
]

logger.debug('URL patterns:\n' + '\n'.join([f"{str(u.pattern)} [{u.name}]" for u in urlpatterns]))
