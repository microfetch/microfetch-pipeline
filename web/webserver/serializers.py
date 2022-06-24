from rest_framework import serializers
from rest_framework.reverse import reverse
from .models import Taxons, Records, RecordDetails, QualifyrReport

import logging

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

    class Meta:
        model = Taxons
        fields = ['id', 'url', 'records', 'post_assembly_filters', 'last_updated', 'time_added']


class RecordDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = RecordDetails
        exclude = ['id', 'record', 'time_fetched']


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
    details = RecordDetailSerializer(read_only=True)

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
