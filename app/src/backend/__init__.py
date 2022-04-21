import csv
import datetime
import logging
from enum import Enum
import click
import os
import pandas
from . import fetch_accession_numbers


logger = logging.getLogger(__file__)
log_format = logging.Formatter(fmt='%(asctime)s %(levelname)s:\t%(message)s')


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


def get_accession_csv_path(taxon_id: str, root_dir: str) -> str:
    """
    Return the path to the Accession metadata CSV for taxon_id
    """
    return os.path.join(root_dir, 'ENA_metadata', f'{taxon_id}.csv')


def update_stage(row: pandas.Series, stage: Stage) -> pandas.Series:
    """
    Update a record to show the next stage to be processed.
    """
    logger.info((
        f"Setting stage from '{row.stage_name}'[{row.stage}]"
        f" to '{stage_names[stage]}'[{stage.value}]."
    ))
    # Avoid SettingWithCopyWarning
    row_dict = row.to_dict()
    row_dict['priority'] = stage.value
    row_dict['stage'] = stage.value
    row_dict['stage_name'] = stage_names[stage]
    row_dict['checkpoint_time'] = datetime.datetime.utcnow().isoformat()
    if stage == Stage.FILTER_ACCESSION_CSV:
        row_dict['last_run_status'] = RunStatus.IN_PROGRESS.value
    if stage == Stage.CREATE_DROPLET_FARM:
        # TODO: This will be set via a callback from digital ocean
        row_dict['last_run_status'] = RunStatus.READY.value
    return pandas.Series(row_dict)


def fetch_accession_csv(row: pandas.Series, context: dict) -> pandas.Series:
    """
    Lookup row.taxon_id in the EBI ENA database and download the metadata CSV.

    Return row updated with new values for the job scheduler.
    """
    csv_path = get_accession_csv_path(row.taxon_id, context['DATA_DIR'])

    # Have to download this fresh every time until EBI ENA gives us access to filtering by time uploaded
    result = fetch_accession_numbers.fetch_records_direct(
        taxon_id=row.taxon_id,
        accession_type='run',
        print_result=False
    )
    result = pandas.DataFrame(result)

    if not os.path.exists(csv_path):
        result.to_csv(csv_path, index=False)
        logger.info(f"Wrote {len(result)} accession results to {csv_path}")
    else:
        existing = pandas.read_csv(csv_path)
        combined = pandas.concat([existing, result])
        combined = combined.drop_duplicates(ignore_index=True)
        combined.to_csv(csv_path, index=False)
        n = len(combined) - len(existing)
        if n > 0:
            logger.info(f"Added {n} new accession results to {csv_path}")
        else:
            logger.info(f"No new accession results found.")

    return update_stage(row, Stage.FILTER_ACCESSION_CSV)


def filter_accession_csv(row: pandas.Series, context: dict) -> pandas.Series:
    """
    Find a local metadata CSV file for row.taxon_id and filter for good candidate records.
    Fetch run accession numbers for those records.
    Return row updated with new values for the job scheduler.
    """
    logger.info(f"filter_accession_csv not yet implemented")
    return update_stage(row, Stage.CREATE_DROPLET_FARM)


def create_droplet_farm(row: pandas.Series, context: dict) -> pandas.Series:
    """
    Find a local metadata CSV file for row.taxon_id and filter for good candidate records.
    Fetch run accession numbers for those records.
    Return row updated with new values for the job scheduler.
    """
    logger.info(f"create_droplet_farm not yet implemented")
    return update_stage(row, Stage.UPDATE_ACCESSION_CSV)
