from rest_framework import serializers
from .models import Taxons, AccessionNumbers, RecordDetails

import logging

logger = logging.getLogger(__file__)


class TaxonSerializer(serializers.ModelSerializer):
    class Meta:
        model = Taxons
        fields = '__all__'


class AccessionNumberSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccessionNumbers
        fields = '__all__'


class RecordDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = RecordDetails
        fields = '__all__'
