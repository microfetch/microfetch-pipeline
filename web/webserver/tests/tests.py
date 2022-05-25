import logging
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from ..models import Taxons, AssemblyStatus
from .factories.factories import AccessionFactory, RecordDetailsFactory

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
        self.assertEqual(Taxons.objects.get().taxon_id, taxon_id)
        self.assertIn('accessions', response.json().keys())


class AccessionTests(APITestCase):
    def setUp(self):
        self.accessions = AccessionFactory.create_batch(100)
        for accession in self.accessions:
            RecordDetailsFactory.create(accession_id_id=accession.accession_id)

        self.accession_in_progress = AccessionFactory.create(
            filtered=True,
            accepted=True,
            completed=False,
            assembled=False
        )
        self.accession_complete = AccessionFactory.create(
            filtered=True,
            accepted=True,
            completed=True,
            assembled=False
        )
        RecordDetailsFactory.create(accession_id_id=self.accession_in_progress.accession_id)
        RecordDetailsFactory.create(accession_id_id=self.accession_complete.accession_id)

        self.assembly_payload = {
            "assembly_result": "success",
            "assembled_genome_url": "ftp://space.place.where.we.keep/genomes/done.gcta",
            "assembly_report_url": "https://web.report.location/my_accession.gcta"
        }

    def test_taxon_has_accessions(self):
        taxon_id = self.accessions[0].taxon_id_id
        url = reverse('taxon', args=(taxon_id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        j = response.json()
        self.assertGreaterEqual(len(j['accessions']), 1)

    def test_accession_view_fails(self):
        # Non-existent URLs should 404
        url = reverse('accession', args=("BAD007247",))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_accession_view(self):
        url = reverse('accession', args=(self.accessions[0].accession_id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_assembly_candidate(self):
        url = reverse('assembly_request')
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        candidate = response.json()
        self.assertEqual(candidate['passed_filter'], True)
        self.assertEqual(candidate['assembly_result'], AssemblyStatus.UNDER_CONSIDERATION.value)

        # confirm candidate
        url = reverse('assembly_confirm', args=(candidate['accession_id'],))
        self.assertEqual(self.client.get(url).status_code, status.HTTP_204_NO_CONTENT)

        # check status is updated to in progress
        url = reverse('accession', args=(candidate['accession_id'],))
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        j = response.json()
        self.assertEqual(j['assembly_result'], AssemblyStatus.IN_PROGRESS.value)

    def test_report_fails(self):
        # We should not be allowed to update an accession not in progress
        url = reverse('accession', args=(self.accession_complete.accession_id,))
        request = self.client.put(url, self.assembly_payload, format='json')
        self.assertEqual(request.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertGreater(len(request.json()['error']), 0)

    def test_report(self):
        url = reverse('accession', args=(self.accession_in_progress.accession_id,))
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
