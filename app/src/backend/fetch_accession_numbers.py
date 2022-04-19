import csv
import sys
from time import sleep
from typing import List, Dict

import click as click
import requests
from retry import retry

from . import fetch_taxon_links as taxon_link

search_url = 'https://www.ebi.ac.uk/ena/portal/api/search'
id_fields = ['run_accession', 'experiment_accession', 'sample_accession', 'secondary_sample_accession']
default_limit = 10000

metadata_fields = [
    'cell_line',  # cell line from which the sample was obtained
    'cell_type',  # cell type from which the sample was obtained
    'collected_by',  # name of the person who collected the specimen
    'collection_date',  # date that the specimen was collected
    'country',  # locality of sample isolation: country names, oceans or seas, followed by regions and localities
    # 'cultivar', # cultivar (cultivated variety) of plant from which sample was obtained
    # 'culture_collection', # identifier for the sample culture including institute and collection code
    'description',  # brief sequence description
    # 'dev_stage', # sample obtained from an organism in a specific developmental stage
    # 'ecotype', # a population within a given species displaying traits that reflect adaptation to a local habitat
    'environmental_sample',
    # identifies sequences derived by direct molecular isolation from an environmental DNA sample
    'first_public',  # date when made public
    'germline',  # the sample is an unrearranged molecule that was inherited from the parental germline
    'identified_by',  # name of the taxonomist who identified the specimen
    # 'isolate', # individual isolate from which sample was obtained
    'isolation_source',  # describes the physical, environmental and/or local geographical source of the sample
    'location',  # geographic location of isolation of the sample
    # 'mating_type', # mating type of the organism from which the sequence was obtained
    'serotype',  # serological variety of a species characterized by its antigenic properties
    'serovar',  # serological variety of a species (usually a prokaryote) characterized by its antigenic properties
    'sex',  # sex of the organism from which the sample was obtained
    'submitted_sex',  # sex of the organism from which the sample was obtained
    # 'specimen_voucher', # identifier for the sample culture including institute and collection code
    # 'strain', # strain from which sample was obtained
    'sub_species',  # name of sub-species of organism from which sample was obtained
    # 'sub_strain', # name or identifier of a genetically or otherwise modified strain from which sample was obtained
    # 'tissue_lib', # tissue library from which sample was obtained
    'tissue_type',  # tissue type from which the sample was obtained
    # 'variety', # variety (varietas, a formal Linnaean rank) of organism from which sample was derived
    'tax_id',  # taxonomic ID
    'scientific_name',  # scientific name
    'sample_alias',  # submitter's name for the sample
    # 'checklist', # checklist name (or ID)
    'center_name',  # Submitting center
    # 'depth', # Depth (m)
    # 'elevation', # Elevation (m)
    # 'altitude', # Altitude (m)
    # 'environment_biome', # Environment (Biome)
    # 'environment_feature', # Environment (Feature)
    # 'environment_material', # Environment (Material)
    # 'temperature', # Temperature (C)
    # 'salinity', # Salinity (PSU)
    'sampling_campaign',  # the activity within which this sample was collected
    'sampling_site',  # the site/station where this sample was collection
    'sampling_platform',  # the large infrastructure from which this sample was collected
    'protocol_label',  # the protocol used to produce the sample
    'project_name',  # name of the project within which the sequencing was organized
    'host',  # natural (as opposed to laboratory) host to the organism from which sample was obtained
    'host_tax_id',  # NCBI taxon id of the host
    'host_status',  # condition of host (eg. diseased or healthy)
    'host_sex',  # physical sex of the host
    'submitted_host_sex',  # physical sex of the host
    'host_body_site',  # name of body site from where the sample was obtained
    'host_gravidity',  # whether or not subject is gravid, including date due or date post-conception where applicable
    # 'host_phenotype', # phenotype of host
    # 'host_genotype', # genotype of host
    # 'host_growth_conditions', # literature reference giving growth conditions of the host
    # 'environmental_package', # MIGS/MIMS/MIMARKS extension for reporting (from environment where the sample was obtained)
    'investigation_type',  # the study type targeted by the sequencing
    'experimental_factor',  # variable aspects of the experimental design
    'sample_collection',  # the method or device employed for collecting the sample
    'sequencing_method',  # sequencing method used
    # 'target_gene', # targeted gene or locus name for marker gene studies
    # 'ph', # pH
    # 'broker_name', # broker name
    'sample_title',  # brief sample title
    'sample_material',  # sample material label
    'taxonomic_identity_marker',  # Taxonomic identity marker
    # 'taxonomic_classification', # Taxonomic classification
    'completeness_score',  # Completeness score (%)
    'contamination_score',  # Contamination score (%)
    'binning_software',  # Binning software
    'lat',  # Latitude
    'lon',  # Longitude
    'sample_capture_status',  # Sample capture status
    'collection_date_submitted',  # Collection date submitted
    'submission_tool'  # Submission tool
]

read_run_fields = [
    'instrument_platform',  # instrument platform used in sequencing experiment
    'instrument_model',  # instrument model used in sequencing experiment
    'library_name',  # sequencing library name
    'library_layout',  # sequencing library layout
    'nominal_length',  # average fragmentation size of paired reads
    'library_strategy',  # sequencing technique intended for the library
    'library_source',  # source material being sequenced
    'library_selection',  # method used to select or enrich the material being sequenced
    'read_count',  # number of reads
    'base_count',  # number of base pairs
    'center_name',  # Submitting center
    'fastq_bytes'  # size (in bytes) of FASTQ files
]

read_experiment_fields = [
    'instrument_platform',  # instrument platform used in sequencing experiment
    'instrument_model',  # instrument model used in sequencing experiment
    'library_name',  # sequencing library name
    'library_layout',  # sequencing library layout
    'nominal_length',  # average fragmentation size of paired reads
    'library_strategy',  # sequencing technique intended for the library
    'library_source',  # source material being sequenced
    'library_selection',  # method used to select or enrich the material being sequenced
    'library_construction_protocol'  # Library construction protocol
]

canned_queries = {
    'read_experiment_identifiers': ['experiment_accession', 'accession', 'sample_accession',
                                    'secondary_sample_accession'],
    'read_run_identifiers': ['experiment_accession', 'run_accession', 'accession', 'sample_accession',
                             'secondary_sample_accession'],
    'sample_identifiers': ['accession', 'sample_accession', 'secondary_sample_accession'],
    'read_experiment_all': ['experiment_accession', 'accession', 'sample_accession',
                             'secondary_sample_accession'] + read_experiment_fields + metadata_fields,
    'read_run_all': ['experiment_accession', 'run_accession', 'accession', 'sample_accession',
                      'secondary_sample_accession'] + read_run_fields + metadata_fields,
}

accession_field_name_map = {'experiment': 'experiment_accession', 'run': 'run_accession', 'sample': 'accession'}


@retry(backoff=2, delay=1, max_delay=1200)
def fetch_data_slice(
        accession_list: List[str],
        accession_type: str,
        result_type: str,
        fields:
        List[str],
        limit=default_limit
) -> List[Dict[str, str]]:
    # accession_list = ['ERX1103730','SRX3894425']
    data = {
        'includeAccessions': ','.join(accession_list),
        'includeAccessionType': accession_type,
        'limit': limit,
        'result': result_type,
        'fields': ','.join(fields),
        'format': 'json',
        'dataPortal': 'ena'
    }
    try:
        r = requests.post(search_url, data=data)
        if r.status_code != 200:
            print(f'POST failed: {r.status_code} {r.reason}\n{r.request}', file=sys.stderr)
            raise IOError
        result = r.json()
    except ValueError:
        print(f'Data failed.')
        raise IOError
    print(f'{len(result)} returned.', file=sys.stderr)
    return result


def fetch(accessions, accession_type, result_type, fields, current_offset=0, search_limit=default_limit) -> List[Dict[str, str]]:
    results = list()
    while current_offset < (len(accessions) - 1):
        print(f'Progress {current_offset} out of {len(accessions)}', file=sys.stderr)
        results.extend(
            fetch_data_slice(
                accessions[current_offset:current_offset + search_limit],
                accession_type,
                result_type,
                fields,
                search_limit
            ))
        current_offset += search_limit
        sleep(2)
    return results


def determine_taxon_result_type(accession_type: str) -> str:
    type_map = {'experiment': 'read_experiment', 'sample': 'sample', 'run': 'read_run'}
    if accession_type in type_map:
        return type_map[accession_type]
    else:
        print(f'{accession_type} not recognised', file=sys.stderr)
        raise Exception


# This is outside the click wrapper to allow direct calling through Python
def fetch_records_direct(
        taxon_id: str,
        accession_type: str = 'experiment',
        result_type: str = 'read_experiment',
        offset: int = 0,
        limit: int = default_limit,
        query: str = 'identifiers',
        print_result: bool = True
) -> list:
    fields = canned_queries[f'{result_type}_{query}']
    taxon_links = taxon_link.fetch(taxon_id, determine_taxon_result_type(accession_type))
    accession_field_name = accession_field_name_map[accession_type]
    accessions = [row[accession_field_name] for row in taxon_links]
    result_list = fetch(accessions, accession_type, result_type, fields=fields, current_offset=offset,
                        search_limit=limit)
    if print_result:
        print_csv(result_list, fields)
    return result_list


@click.command()
@click.option('-t', '--taxon_id', required=True, help='NCBI ID. Will also fetch the subtree.')
@click.option('-a', '--accession-type', required=False, default='experiment', help='experiment (default), sample, run')
@click.option('-r', '--result-type', required=False, default='read_experiment',
              help='read_run, read_experiment, sample')
@click.option('-o', '--offset', required=False, default=0, help='Continue a partial download.')
@click.option('-l', '--limit', required=False, default=default_limit, help='Change limit to number of requests')
@click.option('-q', '--query', required=False, default='identifiers', help=f'all/identifiers')
def fetch_records(taxon_id: str, accession_type: str, result_type: str, offset: int, limit: int, query: str,
                  print_result: bool = True) -> list:
    return fetch_records_direct(
        taxon_id=taxon_id,
        accession_type=accession_type,
        result_type=result_type,
        offset=offset,
        limit=limit,
        query=query,
        print_result=print_result
    )


def print_csv(result_list: List[Dict[str, str]], columns: List[str]):
    writer = csv.writer(sys.stdout)
    writer.writerow(columns)
    for result in result_list:
        writer.writerow(map(lambda x: result.get(x, ""), columns))


if __name__ == '__main__':
    fetch_records()
