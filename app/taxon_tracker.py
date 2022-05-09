import logging
import os
import pandas
import datetime

import pytz
from sqlalchemy import create_engine
from time import sleep
from enum import Enum

from backend import fetch_accession_numbers


# Adapted from https://stackoverflow.com/a/67305494
class DatabaseHandler(logging.Handler):
    backup_logger = None

    def __init__(self, level=0, backup_logger_name=None):
        super().__init__(level)
        if backup_logger_name:
            self.backup_logger = logging.getLogger(backup_logger_name)

    def emit(self, record):
        try:
            msg = record.msg.replace('\n', '\t')
            get_connection().execute((
                f"INSERT INTO {Tables.LOGGING.value} (msg, level, logger_name, trace, create_datetime) "
                f"VALUES ('{msg}', {record.levelno}, '{record.name}', '{record.stack_info}', now())"
            ))
        except:
            pass


logger = logging.getLogger(__file__)
log_format = logging.Formatter(fmt='%(asctime)s %(levelname)s:\t%(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)
logger.addHandler(DatabaseHandler())
logger.setLevel(logging.DEBUG)

DB = None


# Columns and tables are defined in web/webserver/models.py
class Tables(Enum):
    TAXON = 'webserver_taxons'
    ACCESSION = 'webserver_accession_numbers'
    RECORD_DETAILS = 'webserver_record_details'
    LOGGING = 'django_db_logger_statuslog'


class TaxonCols(Enum):
    TAXON_ID = 'taxon_id'
    LAST_UPDATED = 'last_updated'
    TIME_ADDED = 'time_added'


class AccessionCols(Enum):
    TAXON_ID = 'taxon_id_id'  # extra _id courtesy of Django
    ACCESSION_NUMBER = 'accession_number'
    PASSED_FILTER = 'passed_filter'
    TIME_ADDED = 'time_added'
    TIME_FETCHED = 'time_fetched'


class RecordCols(Enum):
    ACCESSION_NUMBER = 'accession_number_id'
    TIME_FETCHED = 'time_fetched'


COLUMNS = {
    Tables.TAXON: TaxonCols,
    Tables.ACCESSION: AccessionCols,
    Tables.RECORD_DETAILS: RecordCols
}


def get_connection() -> object:
    """
    Get the database connection, opening it if necessary.
    """
    global DB
    db_status = 0
    try:
        db_status = DB.status
    except AttributeError:
        pass

    if not db_status:
        db_uri = (
            f"postgresql+psycopg2://"
            f"{os.environ.get('POSTGRES_USER')}:"
            f"{os.environ.get('POSTGRES_PASSWORD')}@"
            f"db:5432/"  # set in docker-compose.yml
            f"{os.environ.get('POSTGRES_DB')}"
        )
        DB = create_engine(db_uri)
    return DB


def get_taxons_to_check() -> pandas.DataFrame:
    taxon_id = COLUMNS[Tables.TAXON].TAXON_ID.value
    last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
    # Fetch accessions for outdated taxon_ids
    return pandas.read_sql(
        sql=(
            f"SELECT {taxon_id} FROM {Tables.TAXON.value} WHERE "
            f"{last_updated} is null OR "
            # Updated over a week ago
            f"date_part('days', now() - {last_updated}) >= 7"  # TODO: soft-code time diff later
        ),
        con=get_connection()
    )


def update_taxons(taxon_ids: pandas.DataFrame) -> None:
    for taxon_id in taxon_ids.taxon_id:
        update_records(taxon_id)
        # TODO: mark taxon_id as updated


def update_records(taxon_id: int) -> None:
    pass
    logger.debug(f"Updating records for taxon id {taxon_id}.")
    # Query ENA for all accession numbers
    all_accessions = query_ENA(taxon_id)

    # Strip out existing accession numbers
    accession_number = COLUMNS[Tables.ACCESSION].ACCESSION_NUMBER.value
    t_id = COLUMNS[Tables.ACCESSION].TAXON_ID.value
    local_records = pandas.read_sql(
        sql=(
            f"SELECT {accession_number} FROM {Tables.ACCESSION.value} WHERE "
            f"{t_id} = {taxon_id}"
        ),
        con=get_connection()
    )

    new_records = all_accessions.loc[~(all_accessions[accession_number].isin(local_records[accession_number]))]

    if len(new_records) == 0:
        logger.debug(f"All ENA {len(all_accessions)} records exist locally.")
    else:
        logger.debug((
            f"ENA has {len(all_accessions)} records. "
            f"{len(local_records)} local records found. "
            f"Fetching {len(new_records)} new records."
        ))

        # Add new records to Accessions table
        new_records[t_id] = [taxon_id for _ in range(len(new_records))]
        # timezone (tz) should match web/settings/settings.py
        new_records[COLUMNS[Tables.ACCESSION].TIME_ADDED.value] = \
            [datetime.datetime.now(tz=pytz.UTC) for _ in range(len(new_records))]
        new_records.to_sql(
            name=Tables.ACCESSION.value,
            con=get_connection(),
            if_exists='append',
            index=False,
            method='multi'
        )

        logger.info(f"Added {len(new_records)} new records for taxon id {taxon_id}.")

    # Filter new records for suitability
    filter_accession_numbers()


def query_ENA(taxon_id: int) -> pandas.DataFrame:
    logger.debug(f"Fetching ENA accession numbers for taxon id {taxon_id}.")
    accession_numbers = fetch_accession_numbers.fetch_records_direct(
        taxon_id=str(taxon_id),
        accession_type='run',
        print_result=False
    )
    df = pandas.DataFrame(accession_numbers)
    df[COLUMNS[Tables.ACCESSION].ACCESSION_NUMBER.value] = df[['accession']]  # rename from backend
    return df[[COLUMNS[Tables.ACCESSION].ACCESSION_NUMBER.value]]


def filter_accession_numbers() -> None:
    """
    Fetch records for any accession numbers without a passed_filter decision and apply filters.
    """
    accession_number = COLUMNS[Tables.ACCESSION].ACCESSION_NUMBER.value
    accession_number_fk = COLUMNS[Tables.RECORD_DETAILS].ACCESSION_NUMBER.value
    passed_filter = COLUMNS[Tables.ACCESSION].PASSED_FILTER.value
    accessions = pandas.read_sql(
        sql=(
            f"SELECT {accession_number}, {passed_filter} FROM {Tables.ACCESSION.value} WHERE "
            f"{passed_filter} is null"
        ),
        con=get_connection()
    )

    if len(accessions) == 0:
        return

    # Fetch detailed records if they don't already exist.
    # This happens as a subset of accession numbers needing filtration because we might want to
    # delete the record details after completed filter application because the complete records
    # are much larger than the accession number summary and are only needed for filtering.
    missing = pandas.read_sql(
        sql=(
            f"SELECT {accession_number} FROM {Tables.ACCESSION.value} WHERE "
            f"{passed_filter} is null AND "
            f"{accession_number} not in (SELECT {accession_number_fk} FROM {Tables.RECORD_DETAILS.value})"
        ),
        con=get_connection()
    )
    records = query_ENA_detail(missing[accession_number])
    records.to_sql(
        name=Tables.RECORD_DETAILS.value,
        con=get_connection(),
        if_exists='append',
        index=False,
        method='multi'
    )

    # Check records against filters
    records = pandas.read_sql(
        sql=(
            f"SELECT * FROM {Tables.RECORD_DETAILS.value} WHERE "
            f"{accession_number_fk} in "
            f"{tuple(accessions[accession_number])}"
        ),
        con=get_connection()
    )
    logger.debug('Records:')
    logger.debug(records)

    records[passed_filter] = True

    # Save results
    new_accessions = records[[accession_number_fk, passed_filter]]
    new_accessions.to_sql(
        name=Tables.ACCESSION.value,
        con=get_connection(),
        if_exists='append',
        index=False,
        method='multi'
    )

    if len(records) > 0:
        logger.info(f"{len(records.loc[records[passed_filter]])}/{len(records)} new records acceptable for assembly.")


def query_ENA_detail(accession_numbers: list) -> pandas.DataFrame:
    logger.debug(f"Fetching {len(accession_numbers)} new ENA record details.")
    df = pandas.DataFrame()
    logger.debug(f"Fetched {len(df)}/{len(accession_numbers)} record details.")
    return df


if __name__ == '__main__':
    """
    Run the next job in the queue and return the amount of time to sleep after completing.
    """
    while True:
        try:
            # Update taxon records
            taxon_ids = get_taxons_to_check()
            update_taxons(taxon_ids)

            # Check for available droplet

            # Check for enough accessions/accessions with long queue times

            # Launch droplet

            # Find the next job

        except BaseException as e:
            logger.error(e)

        sleep(30)
