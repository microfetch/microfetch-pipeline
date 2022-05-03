import datetime
import logging
from enum import IntEnum, Enum
import os
import pandas
from . import fetch_accession_numbers


logger = logging.getLogger(__file__)
log_format = logging.Formatter(fmt='%(asctime)s %(levelname)s:\t%(message)s')


class Route(Enum):
    """
    Keeping fixed file paths DRY
    """
    CSV = "microfetch.csv"
    MAIN_LOG = "main.log"
    SERVER_LOG = "server.log"
    QUEUE_DIR = ".queue"
    ACCESSION_DIR = "ENA_accession_metadata"
    FILTERED_DIR = "ENA_accession_filtered"


class Priority(IntEnum):
    """
    Higher priorities have higher numbers
    """
    STOPPED = -1
    PAUSED = 0
    VERY_LOW = 1
    LOW = 2
    MEDIUM = 3
    HIGH = 4
    VERY_HIGH = 5
    URGENT = 6


class RunStatus(Enum):
    """
    Text descriptions of the run statuses
    """
    NEW = 'new'
    IN_PROGRESS = 'in progress'
    READY = 'ready'
    COMPLETE = 'complete'
    COMPLETE_WITH_WARNING = 'warning'
    ERROR = 'error'


class Stage(IntEnum):
    """
    The next action required on a taxon_id.

    Numbers chosen to be interchangeable with Priority
    """
    UPDATE_ACCESSION_CSV = 1
    FETCH_ACCESSION_CSV = 2
    FILTER_ACCESSION_CSV = 3
    CREATE_DROPLET_FARM = 4
    AWAIT_DATA_COLLECTION = 0  # We pause here because we want to keep doing other stuff while awaiting droplets


class Setting(Enum):
    """
    Names for settings properties
    """
    DROPLET_COUNT = 'DROPLET_COUNT'


# Friendly name for stages
STAGE_NAMES = {
    Stage.FILTER_ACCESSION_CSV: 'filter accession CSV',
    Stage.UPDATE_ACCESSION_CSV: 'update accession CSV',
    Stage.FETCH_ACCESSION_CSV: 'fetch accession CSV',
    Stage.CREATE_DROPLET_FARM: 'create droplet farm'
}

SETTINGS = {
    Setting.DROPLET_COUNT: 2
}


def get_droplets_available(whitelist: list, root_dir: str) -> bool:
    d = os.path.join(root_dir, Route.ACCESSION_DIR.value)
    for f in os.listdir(d):
        base, ext = os.path.splitext(f)
        if base in whitelist:
            df = pandas.read_csv(os.path.join(d, f))
            if 'status' in df.columns:
                return False
    return True


def get_accession_csv_path(taxon_id: str, root_dir: str) -> str:
    """
    Return the path to the Accession metadata CSV for taxon_id
    """
    return os.path.join(root_dir, Route.ACCESSION_DIR.value, f'{taxon_id}.csv')


def get_filtered_csv_path(taxon_id: str, root_dir: str) -> str:
    """
    Return the path to the Filtered CSV for taxon_id
    """
    return os.path.join(root_dir, Route.FILTERED_DIR.value, f'{taxon_id}.csv')


def update_stage(row: pandas.Series, stage: Stage) -> pandas.Series:
    """
    Update a record to show the next stage to be processed.
    """
    logger.info((
        f"Setting stage from '{row.stage_name}'[{row.stage}]"
        f" to '{STAGE_NAMES[stage]}'[{stage.value}]."
    ))
    # Avoid SettingWithCopyWarning
    row_dict = row.to_dict()
    row_dict['priority'] = stage.value
    row_dict['stage'] = stage.value
    row_dict['stage_name'] = STAGE_NAMES[stage]
    row_dict['checkpoint_time'] = datetime.datetime.utcnow().isoformat()
    if stage == Stage.FILTER_ACCESSION_CSV:
        row_dict['last_run_status'] = RunStatus.IN_PROGRESS.value
    if stage == Stage.CREATE_DROPLET_FARM:
        # TODO: This will be set via a callback from digital ocean
        row_dict['last_run_status'] = RunStatus.READY.value
    if stage == Stage.AWAIT_DATA_COLLECTION:
        row_dict['priority'] = Priority.STOPPED.value
    if stage == Stage.UPDATE_ACCESSION_CSV:
        row_dict['last_run_status'] = RunStatus.COMPLETE.value
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
    csv_file = get_accession_csv_path(taxon_id=row.taxon_id, root_dir=context['DATA_DIR'])
    out_file = get_filtered_csv_path(taxon_id=row.taxon_id, root_dir=context['DATA_DIR'])
    logger.info(f"Filtering {csv_file}")

    df = pandas.read_csv(csv_file)
    df = df['accession']

    df.to_csv(out_file, index=False)
    logger.info(f"Wrote to {out_file}")
    return update_stage(row, Stage.CREATE_DROPLET_FARM)


def create_droplet_farm(row: pandas.Series, context: dict) -> pandas.Series:
    """
    Find a local metadata CSV file for row.taxon_id and filter for good candidate records.
    Fetch run accession numbers for those records.
    Return row updated with new values for the job scheduler.
    """
    csv_file = get_filtered_csv_path(taxon_id=row.taxon_id, root_dir=context['DATA_DIR'])
    logger.info(f"Assigning droplets in {csv_file}")

    df = pandas.read_csv(csv_file)
    df['droplet_group'] = [i % SETTINGS[Setting.DROPLET_COUNT] for i in range(len(df))]
    df['droplet_ip'] = ''
    df['status'] = 'awaiting droplet'

    for i in range(SETTINGS[Setting.DROPLET_COUNT]):
        # TODO: spin up the droplets, feed them the accession numbers, and get their IPs
        df.loc[(df.droplet_group == i), 'droplet_ip'] = '127.0.0.1'
        df.loc[(df.droplet_group == i), 'status'] = 'processing'

    df.to_csv(csv_file, index=False)

    # We set to AWAIT_DATA_COLLECTION here.
    # When the droplets have finished they call us back on the webserver and we mark them as ready.
    # Once the data are collected by the BDI servers we can destroy the droplets and mark the whole process as COMPLETE.
    return update_stage(row, Stage.AWAIT_DATA_COLLECTION)


def mark_data_collected(row: pandas.Series, droplet_ip: str, context: dict) -> pandas.Series:
    """
    Mark the data from a droplet as collected
    """
    # TODO: in future we may destroy droplets here
    csv_file = get_filtered_csv_path(taxon_id=row.taxon_id, root_dir=context['DATA_DIR'])
    logger.info(f"Marking droplet {droplet_ip} as collected. (taxon_id={row.taxon_id})")

    df = pandas.read_csv(csv_file)
    df.loc[(df.droplet_ip == droplet_ip), 'status'] = 'collected'

    # Check whether we're all collected
    if all(df.status == 'collected'):
        logger.info(f"All droplet data collected.")
        os.remove(csv_file)
        return update_stage(row=row, stage=Stage.UPDATE_ACCESSION_CSV)

    df.to_csv(csv_file, index=False)
    return row
