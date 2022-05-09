from django.db import models

LENGTH_ACCESSION = 40


class taxons(models.Model):
    taxon_id = models.PositiveBigIntegerField(primary_key=True)
    last_updated = models.DateTimeField(null=True)
    time_added = models.DateTimeField(auto_now_add=True)


class accession_numbers(models.Model):
    taxon_id = models.ForeignKey("taxons", on_delete=models.CASCADE)
    accession_number = models.CharField(primary_key=True, max_length=LENGTH_ACCESSION)
    passed_filter = models.BooleanField(null=True)
    time_added = models.DateTimeField(auto_now_add=True)
    time_fetched = models.DateTimeField(null=True)


class record_details(models.Model):
    accession_number = models.ForeignKey("accession_numbers", on_delete=models.CASCADE)
    time_fetched = models.DateTimeField(auto_now_add=True)
    # ... various other fields as retrieved from ENA database
