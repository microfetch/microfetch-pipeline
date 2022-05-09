import re
import sys
from typing import List

import ena_utils as eu
import extract_experiment_data as ed
import extract_project_data as ep


class SampleData:
    is_wgs = False
    experiment_submitter = ''
    pubmed_ids = list()
    run_id = 'None'

    def __init__(self, species_code, accession, experiment_id, project_id, ena_base_count, submitter, release_date,
                 collection_date, receipt_date, first_created, last_updated, host, geo_loc_name, country, other,
                 lat_lon):
        self.species_code = species_code
        self.accession = accession
        self.experiment_id = experiment_id.replace(',', '-')
        self.project_id = project_id
        self.ena_base_count = ena_base_count
        self.submitter = submitter
        self.last_updated = last_updated
        self.first_created = first_created
        self.receipt_date = receipt_date
        self.release_date = release_date
        self.collection_date = collection_date
        self.host = host
        self.other = other
        self.lat_lon = lat_lon
        self.country = country
        self.geo_loc_name = geo_loc_name

    def csv(self) -> str:
        return '\t'.join(
            [self.species_code, self.accession, self.experiment_id, self.run_id, self.project_id,
             str(self.ena_base_count), self.submitter, self.experiment_submitter, self.release_date,
             self.collection_date, self.receipt_date, self.first_created, self.last_updated, self.host,
             self.geo_loc_name, self.country, self.other, self.lat_lon, str(self.is_wgs), self.pubmed_ids]
        ).replace('"', '')

    @staticmethod
    def wrap(element: str) -> str:
        return '"' + element.replace('"', "'") + '"'


def extract_value_from_sample_attribute(attribute) -> str:
    values = attribute.findall('VALUE')
    if 0 == len(values):
        return 'missing'
    else:
        return values[0].text.strip().replace('\t', ' ')


def parse_id_field(field_string) -> list:
    id_field_parser = re.compile(r'^([A-Za-z0]+)(\d+)$')
    cleaned_ids = list()
    for id_string in field_string.split(','):
        if '-' in id_string:
            print(f'{id_string} needs parsing.', file=sys.stderr)
            (start_record, end_record) = id_string.split('-')
            start_match = id_field_parser.match(start_record)
            prepend = start_match.group(1)
            end_match = id_field_parser.match(end_record)
            start_position = int(start_match.group(2))
            end_position = int(end_match.group(2))
            for number in range(start_position, end_position + 1):
                cleaned_ids.append(prepend + str(number))
        else:
            cleaned_ids.append(id_string.strip())

    return cleaned_ids


def extract_taxon_data(taxon: str, offset: int, length: int) -> List[SampleData]:
    # "https://www.ebi.ac.uk/ena/portal/api/links/taxon?accession=160674&format=json&limit=100&offset=0&result=sample&subtree=true" -H "accept: */*"
    # SAMD00002693 , SAMEA3357538
    # https://www.ebi.ac.uk/ena/portal/api/search?query=sample_accession=%22SAMEA3357538%22&result=sample&format=json&limit=10&fields=all
    # https://www.ebi.ac.uk/ena/portal/api/search?query=sample_accession=%22DRS040022%22&result=sample&format=json&limit=10&fields=all

    # Get the project ID: https://www.ebi.ac.uk/ena/portal/api/links/sample?accession=SAMD00002693&format=json&fields=all&result=read_study
    # Get the experiment ID: https://www.ebi.ac.uk/ena/portal/api/links/sample?accession=SAMD00002693&format=json&fields=all&result=read_experiment
    # This seems to be the best way though, comma-separate IDs: https://www.ebi.ac.uk/ena/browser/api/xml/SAMD00002693

    taxon_query = f"https://www.ebi.ac.uk/ena/portal/api/links/taxon?accession={taxon}&format=json&result=sample&subtree=true" \
                  f"&offset={offset - 1}" \
                  f"&limit={length}"

    # print("Taxon query:", query, file=sys.stderr)
    taxon_samples = eu.download_json(taxon_query)

    results = list()
    sample_ids = [sample['accession'] for sample in taxon_samples]
    print(f'Retrieved {len(sample_ids)}', file=sys.stderr)
    if len(sample_ids) == 0:
        return []

    sample_data_query = f'https://www.ebi.ac.uk/ena/browser/api/xml/{",".join(sample_ids)}'
    samples = eu.download_xml(sample_data_query)

    for sample in samples:
        accession = sample.attrib['accession']

        submitter = ''
        for identifiers in sample.findall('IDENTIFIERS'):
            for identifier in identifiers:
                if identifier.tag == 'SUBMITTER_ID':
                    submitter = identifier.attrib['namespace']

        # Experiment ID
        experiment_id = 'None'
        study_id = 'None'
        for sample_link in sample.findall('SAMPLE_LINKS')[0].findall('SAMPLE_LINK'):
            xref_link = sample_link.findall('XREF_LINK')[0]
            if 'ENA-EXPERIMENT' == xref_link.findall('DB')[0].text:
                experiment_id = xref_link.findall('ID')[0].text
            if 'ENA-STUDY' == xref_link.findall('DB')[0].text:
                study_id = xref_link.findall('ID')[0].text

        ena_base_count = 0
        release_date = 'Unknown'
        collection_date = 'Unknown'
        receipt_date = 'Unknown'
        first_created = 'Unknown'
        last_updated = 'Unknown'
        geo_loc_tag = 'missing'
        country = 'missing'
        lat_long = 'missing'
        other = 'missing'
        host = 'missing'

        for sample_attribute in sample.findall('SAMPLE_ATTRIBUTES')[0]:
            attribute_tag = sample_attribute.findall('TAG')[0].text.upper()

            if attribute_tag == 'ENA-BASE-COUNT':
                ena_base_count = int(extract_value_from_sample_attribute(sample_attribute))

            elif attribute_tag == 'ENA-FIRST-PUBLIC':
                release_date = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag == 'COLLECTION DATE' or attribute_tag == 'COLLECTION_DATE' or attribute_tag == 'COLLECTION-DATE':
                collection_date = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag == 'RECEIPT-DATE':
                receipt_date = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag == 'FIRST-CREATED':
                first_created = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag == 'LAST-UPDATED':
                last_updated = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag == 'GEO_LOC_NAME':
                geo_loc_tag = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag == 'COUNTRY':
                country = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag != 'COUNTRY' and 'COUNTRY' in attribute_tag:
                other = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag == 'LAT_LON':
                lat_long = extract_value_from_sample_attribute(sample_attribute)

            elif attribute_tag == 'HOST':
                host = extract_value_from_sample_attribute(sample_attribute)

        results.append(
            SampleData(taxon, accession, experiment_id, study_id, ena_base_count, submitter, release_date,
                       collection_date, receipt_date, first_created, last_updated, host, geo_loc_tag, country, other,
                       lat_long))
        # print(taxon, accession, experiment_id, str(ena_base_count), release_date, str(has_geo), sep='\t',
        # file=sys.stdout)
    return results


# Script start
get_more = True
current = 1
increment = 200
species = sys.argv[1]
if len(sys.argv) == 3:
    current = int(sys.argv[2])

print(species, file=sys.stderr)
if 1 == current:
    print('\t'.join(
        ['Species', 'Sample ID', 'Experiment ID', 'Run ID', 'Project ID', 'ENA Nt Count', 'Submitter',
         'Experiment Submitter', 'Public Date', 'Collection Date', 'Receipt Date', 'First Created', 'Last Updated',
         'Host', 'Geo_Loc_Name', 'Country', 'Other', 'Lat_Long', 'Possible', 'Pubmed IDs']))

while get_more:
    print('index = ', current, file=sys.stderr)
    taxon_data = extract_taxon_data(species, current, increment)
    result_count = len(taxon_data)
    # print('Results returned = ', result_count, file=sys.stderr)

    if 0 == result_count:
        get_more = False
        break

    experiment_ids = list({item for record in taxon_data for item in record.experiment_id.split('-')})
    project_ids = list({item for record in taxon_data for item in parse_id_field(record.project_id)})

    print('Extracting experiment data')
    wgs_experiments = ed.extract_wgs_experiments(experiment_ids)
    print('Extracting project data')
    project_data = ep.extract_projects(project_ids)

    for record in taxon_data:
        for exp_id in re.split('[-,]', record.experiment_id):
            if exp_id in wgs_experiments:
                experiment = wgs_experiments[exp_id]
                record.experiment_submitter = experiment.submitter
                if experiment.paired and experiment.strategy == 'WGS' and 'ILLUMINA' in experiment.sequencer:
                    record.is_wgs = True
                    record.run_id = experiment.run_id
                    break
        pubmed_ids = list()
        for project_id in re.split('[,]', record.project_id):
            if project_id in project_data:
                project = project_data[project_id]
                pubmed_ids.extend(project.pubmed_xrefs)
        record.pubmed_ids = ','.join(pubmed_ids)

    for record in taxon_data:
        print(record.csv(), file=sys.stdout)

    current += increment
    exit(0)
