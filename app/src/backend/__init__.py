import csv
import datetime
from enum import Enum
import click
import os
import pandas
from . import fetch_accession_numbers


class Priority(Enum):
    STOPPED = 0
    VERY_LOW = 1
    LOW = 2
    MEDIUM = 3
    HIGH = 4
    VERY_HIGH = 5
    URGENT = 6


class RunStatus(Enum):
    NEW = 'new'
    IN_PROGRESS = 'in progress'
    READY = 'ready'
    COMPLETE = 'complete'
    COMPLETE_WITH_WARNING = 'warning'
    ERROR = 'error'


class Stage(Enum):
    """
    Numbers chosen to be interchangeable with Priority
    """
    UPDATE_ACCESSION_CSV = 1
    FETCH_ACCESSION_CSV = 2
    FILTER_ACCESSION_CSV = 3
    CREATE_DROPLET_FARM = 4


stage_names = {
    Stage.FILTER_ACCESSION_CSV: 'filter accession CSV',
    Stage.UPDATE_ACCESSION_CSV: 'update accession CSV',
    Stage.FETCH_ACCESSION_CSV: 'fetch accession CSV',
    Stage.CREATE_DROPLET_FARM: 'create droplet farm'
}


def path(relative_path: str):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', relative_path))


def get_accession_csv_path(taxon_id: str) -> str:
    """
    Return the path to the Accession metadata CSV for taxon_id
    """
    return path(f'data/ENA_metadata/{taxon_id}.csv')


def update_stage(row: pandas.DataFrame, stage: Stage) -> pandas.DataFrame:
    """
    Update a record to show the next stage to be processed.
    """
    row.priority = Stage.FILTER_ACCESSION_CSV.value
    row.stage = Stage.FILTER_ACCESSION_CSV.value
    row.stage_name = stage_names[row.stage]
    row.checkpoint_time = datetime.datetime.utcnow().isoformat()
    return row


def fetch_accession_csv(ctx: click.Context, row: pandas.DataFrame) -> pandas.DataFrame:
    """
    Lookup row.taxon_id in the EBI ENA database and download the metadata CSV.

    Return row updated with new values for the job scheduler.
    """
    result = fetch_accession_numbers.fetch_records(
        taxon_id=row.taxon_id,
        accession_type='run',
        print_result=False
    )
    with open(get_accession_csv_path(row.taxon_id), 'w+') as f:
        writer = csv.writer(f)
        writer.writerows(result)

    return update_stage(row, Stage.UPDATE_ACCESSION_CSV)


def filter_accession_csv(ctx: click.Context, row: pandas.DataFrame) -> pandas.DataFrame:
    """
    Find a local metadata CSV file for row.taxon_id and filter for good candidate records.
    Fetch run accession numbers for those records.

    Return row updated with new values for the job scheduler.
    """
    return update_stage(row, Stage.CREATE_DROPLET_FARM)


def create_droplet_farm(ctx: click.Context, row: pandas.DataFrame) -> pandas.DataFrame:
    """
    Assign the run accession numbers for row.taxon_id to DigitalOcean droplets and launch them.

    Return row updated with new values for the job scheduler.
    """
    return update_stage(row, Stage.UPDATE_ACCESSION_CSV)
