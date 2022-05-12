import logging
import os
import pandas
import datetime
import math
import pytz
import sqlalchemy

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from time import sleep
from enum import Enum
from requests import request

from backend import fetch_accession_numbers


class Settings(Enum):
    ENA_REQUEST_LIMIT = 'ENA_REQUEST_LIMIT'


SETTINGS = {
    Settings.ENA_REQUEST_LIMIT: int(os.environ.get(Settings.ENA_REQUEST_LIMIT.value, '10000'))
}


# Adapted from https://stackoverflow.com/a/67305494
class DatabaseHandler(logging.Handler):
    backup_logger = None

    def __init__(self, level=0, backup_logger_name=None):
        super().__init__(level)
        if backup_logger_name:
            self.backup_logger = logging.getLogger(backup_logger_name)

    def emit(self, record):
        try:
            msg = record.msg.replace('\n', '\t').replace("'", "''")
            with Session(get_engine()) as session:
                session.execute(sqlalchemy.text((
                    f"INSERT INTO {Tables.LOGGING.value} (msg, level, logger_name, trace, create_datetime) "
                    f"VALUES ('{msg}', {record.levelno}, '{record.name}', '{record.stack_info}', now())"
                )))
                session.commit()
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
    ACCESSION = 'webserver_accessionnumbers'
    RECORD_DETAILS = 'webserver_recorddetails'
    LOGGING = 'django_db_logger_statuslog'


class TaxonCols(Enum):
    TAXON_ID = 'taxon_id'
    LAST_UPDATED = 'last_updated'
    TIME_ADDED = 'time_added'


class AccessionCols(Enum):
    TAXON_ID = 'taxon_id_id'  # extra _id courtesy of Django
    ACCESSION_NUMBER = 'accession_number'
    PASSED_FILTER = 'passed_filter'
    FILTER_FAILED = 'filter_failed'
    TIME_ADDED = 'time_added'


class RecordCols(Enum):
    ACCESSION_NUMBER = 'accession_number_id'
    TIME_FETCHED = 'time_fetched'


COLUMNS = {
    Tables.TAXON: TaxonCols,
    Tables.ACCESSION: AccessionCols,
    Tables.RECORD_DETAILS: RecordCols
}


def get_engine() -> sqlalchemy.engine.Engine:
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
        DB = sqlalchemy.create_engine(db_uri, future=True)

    return DB


def get_taxons_to_check() -> pandas.DataFrame:
    taxon_id = COLUMNS[Tables.TAXON].TAXON_ID.value
    last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
    # Fetch accessions for outdated taxon_ids
    with get_engine().connect() as conn:
        df = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT {taxon_id} FROM {Tables.TAXON.value} WHERE "
                f"{last_updated} is null OR "
                # Updated over a week ago
                f"date_part('days', now() - {last_updated}) >= 7"  # TODO: soft-code time diff later
            )),
            con=conn
        )
    return df


def update_taxons(taxon_ids: pandas.DataFrame) -> None:
    if len(taxon_ids) == 0:
        logger.debug("No taxon ids need checking.")
        return

    t_id = COLUMNS[Tables.TAXON].TAXON_ID.value
    last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
    for taxon_id in taxon_ids.taxon_id:
        update_records(taxon_id)
        # mark taxon_id as updated
        with Session(get_engine()) as session:
            session.execute(sqlalchemy.text((
                f"UPDATE {Tables.TAXON.value} SET {last_updated}=now() WHERE {t_id}={taxon_id}"
            )))
            session.commit()


def update_records(taxon_id: int) -> None:
    pass
    logger.info(f"Updating records for taxon id {taxon_id}.")
    # Query ENA for all accession numbers
    all_accessions = query_ENA(taxon_id)

    # Strip out existing accession numbers
    accession_number = COLUMNS[Tables.ACCESSION].ACCESSION_NUMBER.value
    t_id = COLUMNS[Tables.ACCESSION].TAXON_ID.value
    with get_engine().connect() as conn:
        local_records = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT {accession_number} FROM {Tables.ACCESSION.value} WHERE "
                f"{t_id} = {taxon_id}"
            )),
            con=conn
        )

    new_records = all_accessions.loc[~(all_accessions[accession_number].isin(local_records[accession_number]))]

    if len(new_records) == 0:
        logger.info(f"All ENA {len(all_accessions)} records exist locally.")
    else:
        logger.info((
            f"ENA has {len(all_accessions)} records. "
            f"{len(local_records)} local records found. "
            f"Fetching {len(new_records)} new records."
        ))

        # Add new records to Accessions table
        new_records[t_id] = [taxon_id for _ in range(len(new_records))]
        # timezone (tz) should match web/settings/settings.py
        new_records[COLUMNS[Tables.ACCESSION].TIME_ADDED.value] = \
            [datetime.datetime.now(tz=pytz.UTC) for _ in range(len(new_records))]
        with get_engine().connect() as conn:
            new_records.to_sql(
                name=Tables.ACCESSION.value,
                con=conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            conn.commit()

        logger.info(f"Added {len(new_records)} new records for taxon id {taxon_id}.")

    # Filter new records for suitability
    filter_accession_numbers()


def query_ENA(taxon_id: int) -> pandas.DataFrame:
    logger.info(f"Fetching ENA accession numbers for taxon id {taxon_id}.")
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
    filter_failed = COLUMNS[Tables.ACCESSION].FILTER_FAILED.value
    with get_engine().connect() as conn:
        accessions = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT {accession_number}, {passed_filter} FROM {Tables.ACCESSION.value} WHERE "
                f"{passed_filter} is null"
            )),
            con=conn
        )

    if len(accessions) == 0:
        return
    else:
        logger.info(f"Found {len(accessions)} records awaiting filtering.")

    # Fetch detailed records if they don't already exist.
    # This happens as a subset of accession numbers needing filtration because we might want to
    # delete the record details after completed filter application because the complete records
    # are much larger than the accession number summary and are only needed for filtering.
    with get_engine().connect() as conn:
        missing = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT {accession_number} FROM {Tables.ACCESSION.value} WHERE "
                f"{passed_filter} is null AND "
                f"{accession_number} not in (SELECT {accession_number_fk} FROM {Tables.RECORD_DETAILS.value})"
            )),
            con=conn
        )

    if len(missing):
        fetch_ENA_records(missing[accession_number])

    # Check records against filters
    with get_engine().connect() as conn:
        records = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT * FROM {Tables.RECORD_DETAILS.value} WHERE "
                f"{accession_number_fk} in "
                f"{tuple(accessions[accession_number])}"
            )),
            con=conn
        )

    records[passed_filter] = True  # TODO: use actual filter script
    records[f"{passed_filter}_failed"] = ""

    # Save results
    # rename accession number
    new_accessions = records[[accession_number_fk, passed_filter, f"{passed_filter}_failed"]]
    with Session(get_engine()) as session:
        session.execute(
            sqlalchemy.text((
                f"UPDATE {Tables.ACCESSION.value} "
                f"SET {accession_number}=:an, {passed_filter}=:fltr, {filter_failed}=:fail "
                f"WHERE {accession_number}=:an"
            )),
            [{'an': x[0], 'fltr': x[1], 'fail': x[2]} for x in new_accessions.itertuples(index=False)]
        )
        session.commit()

    if len(records) > 0:
        logger.info(f"{len(records.loc[records[passed_filter]])}/{len(records)} new records acceptable for assembly.")


def fetch_ENA_records(accession_numbers: list) -> None:
    logger.info(f"Fetching {len(accession_numbers)} new ENA record details.")
    limit = SETTINGS[Settings.ENA_REQUEST_LIMIT]
    successes = []
    for i in range(math.ceil(len(accession_numbers) / limit)):
        ans = accession_numbers[i * limit:(i + 1) * limit]
        result = request(
            'GET',
            (
                f"https://www.ebi.ac.uk/ena/portal/api/search?"
                f"includeAccessions={','.join(ans)}"
                f"&result=sample&format=json&limit={limit}&fields=all"
            )
        )
        if result.status_code != 200:
            logger.warning((
                f"Error retrieving ENA record details. They will be retrieved later. API Error: {result.text}"
            ))
        else:
            try:
                records = pandas.read_json(result.text)
                # Tidy up a couple of columns
                records[COLUMNS[Tables.RECORD_DETAILS].ACCESSION_NUMBER.value] = records[['accession']]
                records[COLUMNS[Tables.RECORD_DETAILS].TIME_FETCHED.value] = datetime.datetime.now(tz=pytz.UTC)
                with get_engine().connect() as conn:
                    records.to_sql(
                        name=Tables.RECORD_DETAILS.value,
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    conn.commit()

                successes = [*successes, *ans]

            except BaseException as e:
                logger.error((
                    f"Error saving ENA record details. They will be retrieved later. Error: {e}"
                ))

    logger.info(f"Fetched {len(successes)}/{len(accession_numbers)} record details.")


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
