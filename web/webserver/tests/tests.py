import logging
from faker import Factory
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from ..models import Taxons, AccessionNumbers, RecordDetails
from .factories.factories import AccessionFactory

logger = logging.getLogger(__file__)


class TaxonTests(APITestCase):
    def test_add_taxon(self):
        """
        Ensure we can add a taxon for tracking.
        """
        taxon_id = 755
        url = reverse('taxon', args=(taxon_id,))
        response = self.client.put(url)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Taxons.objects.count(), 1)
        self.assertEqual(Taxons.objects.get().taxon_id, taxon_id)
        self.assertIn('accessions', response.json().keys())


class AccessionTests(APITestCase):
    def setUp(self):
        self.accessions = AccessionFactory.create_batch(100)

    def test_taxon_has_accessions(self):
        taxon_id = self.accessions[0].taxon_id
        url = reverse('taxon', args=(taxon_id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        j = response.json()
        self.assertGreaterEqual(len(j['accessions']), 1)
