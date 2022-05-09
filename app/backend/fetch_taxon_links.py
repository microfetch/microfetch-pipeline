import pprint
import sys
from typing import List

import click
import requests
from retry import retry

taxon_url = 'https://www.ebi.ac.uk/ena/portal/api/links/taxon'


@retry(backoff=2, delay=1, max_delay=1200)
def fetch(taxon_id: str, link_type: str) -> List[str]:
    print(f'Fetching IDs for {taxon_id}', file=sys.stderr)
    try:
        r = requests.get(taxon_url, params={
            'accession': taxon_id,
            'limit': 0,
            'result': link_type,
            'format': 'json',
            'subtree': True
        })
        if r.status_code != 200:
            print(f'GET failed: {r.status_code}', file=sys.stderr)
            raise IOError
        else:
            print(f'Got the experiment records by taxon ID.', file=sys.stderr)
        result = r.json()
    except ValueError:
        print(f'Data failed.', file=sys.stderr)
        raise IOError
    return result


@click.command()
@click.option('--taxon_id', help='NCBI ID. Will also fetch the subtree.')
@click.option('--link_type', default='sample', help='The type of link, e.g. read_experiment, read_run, sample (default).')
def fetch_links(taxon_id: str, link_type: str):
    links = fetch(taxon_id, link_type)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(links)


if __name__ == '__main__':
    fetch_links()
