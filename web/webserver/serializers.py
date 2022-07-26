from rest_framework import serializers
from rest_framework.reverse import reverse
from .models import Taxons, Records, QualifyrReport

import logging
import os

logger = logging.getLogger(__file__)


class TaxonSerializer(serializers.HyperlinkedModelSerializer):
    url = serializers.HyperlinkedIdentityField(
        view_name='taxon-detail',
        lookup_field='id',
        lookup_url_kwarg='pk',
        read_only=True
    )
    records = serializers.HyperlinkedRelatedField(
        view_name='record-detail',
        lookup_field='id',
        lookup_url_kwarg='pk',
        many=True,
        read_only=True
    )
    last_updated = serializers.DateTimeField(read_only=True)
    time_added = serializers.DateTimeField(read_only=True)
    short_record_link = serializers.SerializerMethodField(method_name='get_short_record_link')

    def get_short_record_link(self, instance) -> str:
        query = (
            f"https://www.ebi.ac.uk/ena/portal/api/search?"
            f"format=json"
            f"&result=read_run"
            f"&fields=sample_accession,experiment_accession,run_accession"
            f",lat,lon,country,collection_date,fastq_ftp,first_public"
            # Core query
            f"&query=tax_eq({instance.id})"
            # Filters
            f"%20AND%20base_count%3C{int(os.environ.get('MIN_BASE_PAIRS', '0'))}"
        )
        return query + f"%20AND%20{instance.additional_query_parameters}"

    class Meta:
        model = Taxons
        fields = [
            'id',
            'url',
            'records',
            'post_assembly_filters',
            'last_updated',
            'time_added',
            'additional_query_parameters',
            'short_record_link'
        ]


class QualifyrReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = QualifyrReport
        exclude = ['id', 'record']


class RecordSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = Records
        exclude = []

    id = serializers.ReadOnlyField(read_only=True)
    taxon = serializers.HyperlinkedRelatedField(
        view_name='taxon-detail',
        lookup_field='id',
        lookup_url_kwarg='pk',
        read_only=True
    )
    url = serializers.HyperlinkedIdentityField(
        view_name='record-detail',
        lookup_field='id',
        lookup_url_kwarg='pk',
        read_only=True
    )
    qualifyr_report = QualifyrReportSerializer(read_only=True)
    action_links = serializers.SerializerMethodField(read_only=True)

    def get_action_links(self, obj):
        return {
            'register_assembly_attempt': reverse(
                'record-register-assembly-attempt',
                args=(obj.id,),
                request=self.context['request']
            ),
            'report_assembly_result': reverse(
                'record-report-assembly-result',
                args=(obj.id,),
                request=self.context['request']
            ),
            'report_screening_result': reverse(
                'record-report-screening-result',
                args=(obj.id,),
                request=self.context['request']
            ),
        }
