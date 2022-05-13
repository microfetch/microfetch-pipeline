from django.db import models

LENGTH_ACCESSION = 40
LENGTH_MEDIUM = 256
LENGTH_LONG = 1024


class Taxons(models.Model):
    taxon_id = models.PositiveBigIntegerField(primary_key=True)
    last_updated = models.DateTimeField(null=True)
    time_added = models.DateTimeField(auto_now_add=True)


class AccessionNumbers(models.Model):
    taxon_id = models.ForeignKey("Taxons", on_delete=models.DO_NOTHING)
    experiment_accession = models.CharField(primary_key=True, max_length=LENGTH_ACCESSION)
    accession = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    run_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    sample_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    secondary_sample_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    passed_filter = models.BooleanField(null=True)
    filter_failed = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    time_added = models.DateTimeField(auto_now_add=True)


class RecordDetails(models.Model):
    experiment_accession = models.ForeignKey("AccessionNumbers", on_delete=models.DO_NOTHING)
    time_fetched = models.DateTimeField(auto_now_add=True)
    # Fields as retrieved from ENA database
    accession = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    altitude = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    assembly_quality = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    assembly_software = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    base_count = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    binning_software = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    bio_material = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    broker_name = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    cell_line = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    cell_type = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    center_name = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    checklist = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    collected_by = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    collection_date = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    collection_date_submitted = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    completeness_score = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    contamination_score = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    country = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    cultivar = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    culture_collection = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    depth = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    dev_stage = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    description = models.CharField(null=True, max_length=LENGTH_LONG)
    ecotype = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    elevation = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    environment_biome = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    environment_feature = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    environment_material = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    environmental_package = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    environmental_sample = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    experiment_alias = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    experiment_title = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    experimental_factor = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    first_created = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    first_public = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    germline = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host_body_site = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host_genotype = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host_growth_conditions = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host_gravidity = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host_phenotype = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host_sex = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host_status = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    host_tax_id = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    identified_by = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    instrument_model = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    instrument_platform = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    investigation_type = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    isolate = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    isolation_source = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    last_updated = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    lat = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    lon = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    library_construction_protocol = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    library_layout = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    library_name = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    library_selection = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    library_source = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    library_strategy = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    location = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    mating_type = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    nominal_length = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    nominal_sdev = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    parent_study = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    ph = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    project_name = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    protocol_label = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    read_count = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    run_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    run_alias = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    salinity = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sample_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    sample_alias = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sample_capture_status = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sample_collection = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sample_description = models.CharField(null=True, max_length=LENGTH_LONG)
    sample_material = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sample_title = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sampling_campaign = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sampling_platform = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sampling_site = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    scientific_name = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    secondary_sample_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    secondary_study_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    sequencing_method = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    serotype = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    serovar = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sex = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    specimen_voucher = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    strain = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    study_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    study_alias = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    study_title = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sub_species = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    sub_strain = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    submission_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    submission_tool = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    submitted_format = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    submitted_host_sex = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    submitted_sex = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    target_gene = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    taxonomic_classification = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    taxonomic_identity_marker = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    tax_id = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    taxonomy = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    temperature = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    tissue_lib = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    tissue_type = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    variety = models.CharField(null=True, max_length=LENGTH_MEDIUM)
