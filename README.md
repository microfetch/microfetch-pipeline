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
1. Request a record from `GET api/request_assembly_candidate/`
2. Accept the duty of assembling that record `GET api/confirm_assembly_candidate/{record_id}`
3. Download the data at the `fastq_ftp` links included in the server response at step 1
4. Attempt assembly of the genome
5. Submit the result ('fail' or 'success') and links to a full report and the assembled genome
   (if applicable) using `PUT api/record/{record_id}`

The content of step 5 will be a JSON file similar to:

```json5
{
  "assembly_result": "success",
  "assembled_genome_url": "https://some.repository.url/some/path/genome.gtca",
  "assembly_error_report_url": "https://some.repository.url/some/path/report.html", 
  "qualifyr_report": {
   // key-value pairs for the qualifyr report of the assembly quality
  }
}
```
