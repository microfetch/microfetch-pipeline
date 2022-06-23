import logging
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from ..models import Taxons, AssemblyStatus
from .factories.factories import RecordFactory, RecordDetailsFactory

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


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
        self.assertEqual(Taxons.objects.get().id, taxon_id)
        self.assertIn('records', response.json().keys())


class RecordTests(APITestCase):
    def setUp(self):
        self.records = RecordFactory.create_batch(100)
        for record in self.records:
            RecordDetailsFactory.create(record_id=record.id)

        self.record_in_progress = RecordFactory.create(
            filtered=True,
            accepted=True,
            completed=False,
            assembled=False
        )
        self.record_complete = RecordFactory.create(
            filtered=True,
            accepted=True,
            completed=True,
            assembled=False
        )
        RecordDetailsFactory.create(record_id=self.record_in_progress.id)
        RecordDetailsFactory.create(record_id=self.record_complete.id)

        self.assembly_payload = {
            "assembly_result": "success",
            "assembled_genome_url": "ftp://space.place.where.we.keep/genomes/done.gcta",
            "assembly_error_report_url": "https://web.report.location/my_accession.gcta"
        }

    def test_taxon_has_records(self):
        taxon_id = self.records[0].taxon_id
        url = reverse('taxon', args=(taxon_id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        j = response.json()
        self.assertGreaterEqual(len(j['records']), 1)

    def record_view_fails(self):
        # Non-existent URLs should 404
        url = reverse('record', args=("BAD007247",))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def record_view(self):
        url = reverse('record', args=(self.records[0].id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_assembly_candidate(self):
        url = reverse('assembly_request')
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        candidate = response.json()
        self.assertEqual(candidate['passed_filter'], True)
        self.assertEqual(candidate['assembly_result'], AssemblyStatus.WAITING.value)

        # confirm candidate
        url = reverse('assembly_confirm', args=(candidate['id'],))
        self.assertEqual(self.client.get(url).status_code, status.HTTP_204_NO_CONTENT)

        # check status is updated to in progress
        url = reverse('record', args=(candidate['id'],))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        j = response.json()
        self.assertEqual(j['assembly_result'], AssemblyStatus.IN_PROGRESS.value)

    def test_report_fails(self):
        # We should not be allowed to update an record not in progress
        url = reverse('record', args=(self.record_complete.id,))
        request = self.client.put(url, self.assembly_payload, format='json')
        self.assertEqual(request.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertGreater(len(request.json()['error']), 0)

    def test_report(self):
        url = reverse('record', args=(self.record_in_progress.id,))
        self.assertEqual(
            self.client.put(url, self.assembly_payload, format='json').status_code,
            status.HTTP_204_NO_CONTENT
        )

        # Check updates saved
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        j = response.json()
        for k in self.assembly_payload.keys():
            self.assertEqual(j[k], self.assembly_payload[k])
