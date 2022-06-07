from enum import Enum
import sqlalchemy
import os


DB = None


# Columns and tables are defined in web/webserver/models.py
# The lists below are not exhaustive, they reflect items likely to be useful in the code.
class Tables(Enum):
    TAXON = 'webserver_taxons'
    RECORD = 'webserver_records'
    RECORD_DETAILS = 'webserver_recorddetails'
    LOGGING = 'django_db_logger_statuslog'


class TaxonCols(Enum):
    ID = 'id'
    LAST_UPDATED = 'last_updated'
    TIME_ADDED = 'time_added'


class RecordCols(Enum):
    ID = 'id'
    TAXON = 'id'
    EXPERIMENT_ACCESSION = 'experiment_accession'
    RUN_ACCESSION = 'run_accession'
    SAMPLE_ACCESSION = 'sample_accession'
    FASTQ_FTP = 'fastq_ftp'
    PASSED_FILTER = 'passed_filter'
    FILTER_FAILED = 'filter_failed'
    TIME_FETCHED = 'time_fetched'
    WAITING_SINCE = 'waiting_since'
    ASSEMBLY_RESULT = 'assembly_result'


class DetailCols(Enum):
    RECORD = 'id'
    TIME_FETCHED = 'time_fetched'
    SAMPLE_ACCESSION = 'sample_accession'
    EXPERIMENT_ACCESSION = 'experiment_accession'
    RUN_ACCESSION = 'run_accession'
    FASTQ_FTP = 'fastq_ftp'


COLUMNS = {
    Tables.TAXON: TaxonCols,
    Tables.RECORD: RecordCols,
    Tables.RECORD_DETAILS: DetailCols
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
        # postgresql+psycopg2://postgres:postgres@db:5432/postgres
        db_uri = (
            f"postgresql+psycopg2://"
            f"{os.environ.get('POSTGRES_USER')}:"
            f"{os.environ.get('POSTGRES_PASSWORD')}@"
            f"db:5432/"  # set in docker-compose.yml
            f"{os.environ.get('POSTGRES_DB')}"
        )
        DB = sqlalchemy.create_engine(db_uri, future=True)

    return DB
