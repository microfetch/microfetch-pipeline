import logging
import os
import pandas

from sqlalchemy import create_engine
from time import sleep
from enum import Enum


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
    time_added = COLUMNS[Tables.TAXON].TIME_ADDED.value
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
    logger.debug(f"ENA has {len(all_accessions)} records.")

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
    new_records = [n for n in all_accessions if n not in local_records]

    if len(new_records) == 0:
        logger.debug(f"All ENA {len(all_accessions)} records exist locally.")
        return

    logger.debug((
        f"ENA has {len(all_accessions)} records. "
        f"{len(local_records)} local records found. "
        f"Fetching {len(new_records)} new records."
    ))

    # Add new records to Accessions table
    pandas.DataFrame({t_id: taxon_id, accession_number: new_records})\
        .to_sql(name=Tables.ACCESSION.value, con=get_connection(), index=False)

    # Fetch detailed record for new accession numbers
    records = query_ENA_detail(new_records)
    records.to_sql(name=Tables.RECORD_DETAILS.value, con=get_connection(), index=False)
    logger.info(f"Added {len(new_records)} new records for taxon id {taxon_id}.")

    # Filter new records for suitability
    filter_accession_numbers(taxon_id)


def query_ENA(taxon_id: int) -> pandas.DataFrame:
    logger.debug(f"Fetching ENA accession numbers for taxon id {taxon_id}.")
    return pandas.DataFrame()


def query_ENA_detail(accession_numbers: list) -> pandas.DataFrame:
    logger.debug(f"Fetching new ENA record details.")
    return pandas.DataFrame()


def filter_accession_numbers(taxon_id: int) -> None:
    accession_number = COLUMNS[Tables.ACCESSION].ACCESSION_NUMBER.value
    t_id = COLUMNS[Tables.ACCESSION].TAXON_ID.value
    passed_filter = COLUMNS[Tables.ACCESSION].PASSED_FILTER.value
    accessions = pandas.read_sql(
        sql=(
            f"SELECT {accession_number, passed_filter} FROM {Tables.ACCESSION.value} WHERE "
            f"{t_id} = {taxon_id} AND {passed_filter} is null"
        ),
        con=get_connection()
    )
    records = pandas.read_sql(
        sql=(
            f"SELECT * FROM {Tables.RECORD_DETAILS} WHERE "
            f"{COLUMNS[Tables.RECORD_DETAILS].ACCESSION_NUMBER.value} in "
            f"({', '.join(accessions[accession_number])})"
        ),
        con=get_connection()
    )
    records[passed_filter] = True

    new_accessions = records.loc[:, [accession_number, passed_filter]]
    new_accessions.to_sql(name=Tables.ACCESSION, con=get_connection(), index=False)
    logger.info(f"{len(records.loc[records[passed_filter]])}/{len(records)} new records acceptable for assembly.")


if __name__ == '__main__':
    """
    Run the next job in the queue and return the amount of time to sleep after completing.
    """
    while True:
        try:
            # Update taxon records
            taxon_ids = get_taxons_to_check()
            logger.debug(taxon_ids)
            update_taxons(taxon_ids)

            # Check for available droplet

            # Check for enough accessions/accessions with long queue times

            # Launch droplet

            # Find the next job

        except BaseException as e:
            logger.error(e)

        sleep(30)
