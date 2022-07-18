# microfetch pipeline

[![Web API Tests](https://github.com/OxfordRSE/microfetch-pipeline/actions/workflows/test.yml/badge.svg)](https://github.com/OxfordRSE/microfetch-pipeline/actions/workflows/test.yml)

## Introduction

The microfetch procedure is used to assemble genomes from genomic data 
hosted on the European Nucleotide Archive (ENA).
The code in this repository provides an API wrapper to allow for automated
assembly of new ENA records.
This repository consists of two programs: a web API and an ENA crawler.
The web API allows users to add additional taxonomic identifiers to the list
of taxon_ids that are being tracked,
and allows assembly programs to acquire ENA data links and report assembly results.
The ENA crawler syncs local and ENA records for tracked taxon_ids.

## REST API

The REST API (`web/`) is designed for use by humans wishing to add taxon_ids for tracking 
and by assembler programs to determine which records to assemble and to report their results.
The Swagger UI documentation for the API can be viewed at `/swagger-ui/`. 
The API is written in Django.

## ENA crawler

`app/taxon_tracker.py` iterates through all taxon_ids submitted for tracking, and ensures that
the database has a copy of all records for that taxon_id.
Any missing records are downloaded and tested against a set of filters to assess likely suitability
for assembly. 
Records passing filters may be requested by assembly programs.

The ENA crawler process also handles resetting expired assembly requests.

## Assembly Program interface

Assembly programs should obey the following steps:
1. Request a record from `GET records/awaiting_assembly`
2. Accept the duty of assembling that record `GET records/{record_id}/register_assembly_attempt/`
   * This link will appear in the record's `action_links` field as `register_assembly_attempt`
3. Download the data at the `fastq_ftp` links included in the server response at step 1
4. Attempt assembly of the genome
5. Check the assembly result metrics against the taxon's post assembly filters.
   * These can be obtained by looking up the taxon in the record's `taxon` field.
   The filters are listed in the taxon's JSON as `post_assembly_filters`
6. Submit the result ('fail' or 'success') and links to a full report and the assembled genome
   (if applicable) using `POST record/{record_id}/report_assembly_result/`
   * This link will appear in the record's `action_links` as `report_assembly_result`

The content of step 5 will be a JSON file similar to:

```json5
{
  "assembly_result": "success",
  "assembled_genome_url": "https://some.repository.url/some/path/genome.gtca",
  "assembled_genome_sha1": "8c8f4a123f4a3e53d4cbac08a2e62fbd760083cb",
   "assembly_error_report_url": "https://some.repository.url/some/path/report.html",
  "assembly_error_process": "GENOME_SIZE_ESTIMATION (ERR4559455)",
  "assembly_error_exit_code": "1",
  "assembly_error_stdout": "Checking size\n",
  "assembly_error_stderr": "Error: Error checking size!", 
  "qualifyr_report": {
   // key-value pairs for the qualifyr report of the assembly quality
  }
}
```

## Human screening

Once records have been downloaded, filtered, assembled, checked, and uploaded,
there is one final step before they are deemed ready for deployment.
That step is human screening. 

All records awaiting screening can be obtained from the endpoint 
`records/awaiting_screening/`.

Screening results can be reported for each record by POSTing to the record's
`action_links.report_screening_result` endpoint, usually
`records/{record_id}/report_screening_result/`.
The POST request body should be a JSON with:

```json5
{
   "passed_screening": "REQUIRED. Boolean, True if screening is passed, False otherwise.",
   "screening message": "Optional string field for more details. If screening failed, use this to explain why."
}
```
