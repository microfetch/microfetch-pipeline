import datetime
import logging
import os
import re
import click
import pandas

from sqlalchemy import create_engine
from time import sleep
from enum import Enum

logger = logging.getLogger(__file__)

DB = None


# Columns and tables are defined in web/webserver/models.py
class Tables(Enum):
    TAXON = 'webserver_taxons'
    ACCESSION = 'webserver_accession_numbers'
    RECORD_DETAILS = 'webserver_record_details'


class TaxonCols(Enum):
    TAXON_ID = 'taxon_id'
    LAST_UPDATED = 'last_updated'
    TIME_ADDED = 'time_added'


class AccessionCols(Enum):
    TAXON_ID = 'taxon_id'
    ACCESSION_NUMBER = 'accession_number'
    PASSED_FILTER = 'passed_filter'
    TIME_ADDED = 'time_added'
    TIME_FETCHED = 'time_fetched'


class RecordCols(Enum):
    ACCESSION_NUMBER = 'accession_number'
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


if __name__ == '__main__':
    """
    Run the next job in the queue and return the amount of time to sleep after completing.
    """
    while True:
        try:
            last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
            time_added = COLUMNS[Tables.TAXON].TIME_ADDED.value
            # Fetch accessions for outdated taxon_ids
            taxon_ids = pandas.read_sql(
                sql=(
                    f"SELECT * FROM {Tables.TAXON.value} WHERE "
                    # Updated over a week ago
                    f"date_part('days', now() - {last_updated}) >= 7 OR "  # TODO: soft-code time diff later
                    # OR Timestamps equal
                    f"{last_updated} - {time_added} = make_interval()"
                ),
                con=get_connection()
            )
            logger.debug(taxon_ids)

            # Check for available droplet

            # Check for enough accessions/accessions with long queue times

            # Launch droplet

            # Find the next job

        except BaseException as e:
            logger.error(e)

        sleep(30)
