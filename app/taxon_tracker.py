import logging
import pandas
import datetime
import math
import pytz
import sqlalchemy

from typing import Callable
from sqlalchemy.orm import Session
from time import sleep
from requests import request

from taxon_tracker import filters
from taxon_tracker.settings import Settings
from taxon_tracker.logging import DatabaseHandler, stream_handler, log_format
from taxon_tracker.database import Tables, COLUMNS, get_engine


logger = logging.getLogger(__file__)
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)
logger.addHandler(DatabaseHandler())
logger.setLevel(logging.DEBUG)


class ENA_Error(BaseException):
    pass


def rate_limit(list_like: list, fun: Callable, rate: int) -> any:
    """
    Apply a function to a list in segments of rate length.
    fun() should return a list when handed a list.
    """
    processed_list = []
    for i in range(math.ceil(len(list_like) / rate)):
        list_slice = list_like[i * rate:(i + 1) * rate]
        processed_list = [*processed_list, *fun(list_slice)]

    return processed_list


def get_taxons_to_check() -> pandas.DataFrame:
    taxon_id = COLUMNS[Tables.TAXON].ID.value
    last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
    # Fetch records for outdated taxon_ids
    with get_engine().connect() as conn:
        df = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT {taxon_id} FROM {Tables.TAXON.value} WHERE "
                f"{last_updated} IS NULL OR "
                # Updated over a week ago
                f"DATE_PART('{Settings.TAXON_UPDATE_UNITS.value}', NOW() - {last_updated}) >= "
                f"{Settings.TAXON_UPDATE_N.value}"
            )),
            con=conn
        )
    return df


def update_taxons(taxon_ids: pandas.DataFrame) -> None:
    if len(taxon_ids) == 0:
        logger.debug("No taxon ids need checking.")
        return

    t_id = COLUMNS[Tables.TAXON].ID.value
    last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
    for taxon_id in taxon_ids.taxon_id:
        update_records(taxon_id)
        # mark id as updated
        with Session(get_engine()) as session:
            session.execute(sqlalchemy.text((
                f"UPDATE {Tables.TAXON.value} SET {last_updated}=NOW() WHERE {t_id}={taxon_id}"
            )))
            session.commit()


def update_records(taxon_id: int) -> None:
    logger.info(f"Updating records for taxon id {taxon_id}.")
    # Query ENA for all record numbers
    query_ENA(taxon_id)

    # Filter new records for suitability
    filter_records()


def query_ENA(taxon_id: int) -> None:
    logger.info(f"Fetching ENA record numbers for taxon id {taxon_id}.")
    records = None
    offset = 0
    limit = Settings.ENA_REQUEST_LIMIT.value
    while True:
        url = (
            f"https://www.ebi.ac.uk/ena/portal/api/links/taxon?"
            f"record={taxon_id}"
            f"&format=json"
            f"&limit={limit}"
            f"&offset={offset}"
            f"&result=read_run"
            f"&subtree=true"
        )
        logger.debug(url)
        result = request('GET', url)

        if result.status_code == 204:
            # Undocumented, but ENA sends 204 when asking for out-of-range results
            break
        if result.status_code != 200:
            raise ENA_Error(result.text)

        df = pandas.read_json(result.text)

        if type(df) is not pandas.DataFrame:
            break
        if records is None:
            records = df
        else:
            records = pandas.concat([df, records], ignore_index=True)

        if len(df) >= limit:
            offset = offset + limit
        else:
            break

    logger.debug(f"Found {len(records)} record numbers for taxon_id {taxon_id}.")

    t_id = COLUMNS[Tables.RECORD].ID.value
    run_accession = COLUMNS[Tables.RECORD].RUN_ACCESSION.value

    with get_engine().connect() as conn:
        existing_records = pandas.read_sql(sqlalchemy.text((
            f"SELECT {run_accession} FROM {Tables.RECORD.value} WHERE {t_id} = {taxon_id}"
        )), con=conn)

    missing = records.loc[~records[run_accession].isin(existing_records[run_accession])]

    logger.info(f"{len(existing_records)}/{len(records)} ENA records exist locally.")

    # Fetch records if they don't already exist.
    if len(missing) > 0:
        fetch_ENA_records(missing, taxon_id)


def fetch_ENA_records(records: pandas.DataFrame, taxon_id: int) -> None:
    logger.info(f"Fetching records for {len(records)} records.")
    limit = Settings.ENA_REQUEST_LIMIT.value
    response_limit = 0
    successes = []
    for i in range(math.ceil(len(records) / limit)):
        ans = records.iloc[i * limit:(i + 1) * limit]
        url = "https://www.ebi.ac.uk/ena/portal/api/search"
        data = {
            'includeAccessions': f"{','.join(ans[COLUMNS[Tables.RECORD].RUN_ACCESSION.value])}",
            'result': 'read_run',
            'format': 'json',
            'limit': response_limit,
            'fields': 'all'
        }
        logger.debug(f"Fetching records {i * limit}:{(i + 1) * limit}")

        # TODO: remove debugging fwrite
        with open('.request', 'w+') as f:
            f.write(f"POST {url}\n\n{data}")
        result = request('POST', url=url, data=data)
        if result.status_code != 200:
            logger.warning((
                f"Error retrieving ENA record details. They will be retrieved later. API Error: {result.text}"
            ))
        else:
            try:
                records = pandas.read_json(result.text)
                if len(records) == 0:
                    logger.warning(f"Empty result set retrieved.")
                    continue

                # Tidy up a couple of columns
                record_ids = []
                for r in range(len(records)):
                    row = records.iloc[r].to_dict()
                    record_ids.append((
                        f"{row[COLUMNS[Tables.RECORD_DETAILS].SAMPLE_ACCESSION.value]}_"
                        f"{row[COLUMNS[Tables.RECORD_DETAILS].EXPERIMENT_ACCESSION.value]}_"
                        f"{row[COLUMNS[Tables.RECORD_DETAILS].RUN_ACCESSION.value]}"
                    ))
                records[COLUMNS[Tables.RECORD_DETAILS].ID.value] = record_ids
                records[COLUMNS[Tables.RECORD_DETAILS].TIME_FETCHED.value] = datetime.datetime.now(tz=pytz.UTC)

                # Slim table for saving space
                slim_records = records.filter(items=[
                    COLUMNS[Tables.RECORD_DETAILS].ID.value,
                    COLUMNS[Tables.RECORD_DETAILS].SAMPLE_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].RUN_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].EXPERIMENT_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].TIME_FETCHED.value,
                    COLUMNS[Tables.RECORD_DETAILS].FASTQ_FTP.value
                ])
                slim_records = slim_records.copy()
                slim_records = slim_records.rename(columns={
                    COLUMNS[Tables.RECORD_DETAILS].ID.value:
                        COLUMNS[Tables.RECORD].ID.value,
                    COLUMNS[Tables.RECORD_DETAILS].SAMPLE_ACCESSION.value:
                        COLUMNS[Tables.RECORD].SAMPLE_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].RUN_ACCESSION.value:
                        COLUMNS[Tables.RECORD].RUN_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].EXPERIMENT_ACCESSION.value:
                        COLUMNS[Tables.RECORD].EXPERIMENT_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].TIME_FETCHED.value:
                        COLUMNS[Tables.RECORD].TIME_FETCHED.value,
                    COLUMNS[Tables.RECORD_DETAILS].FASTQ_FTP.value:
                        COLUMNS[Tables.RECORD].FASTQ_FTP.value
                })
                slim_records[COLUMNS[Tables.RECORD].ID.value] = taxon_id

                with get_engine().connect() as conn:
                    records.to_sql(
                        name=Tables.RECORD_DETAILS.value,
                        con=conn,
                        if_exists='append',
                        index=False
                    )
                    slim_records.to_sql(
                        name=Tables.RECORD.value,
                        con=conn,
                        if_exists='append',
                        index=False
                    )
                    conn.commit()

                successes = [*successes, *ans]

            except BaseException as e:
                logger.error((
                    f"Error saving ENA record details. They will be retrieved later. Error: {e}"
                ))

    logger.info(f"Fetched {len(successes)}/{len(records)} record details.")


def filter_records() -> None:
    """
    Fetch records for any record numbers without a passed_filter decision and apply filters.
    """
    record = COLUMNS[Tables.RECORD].ID.value
    accession_fk = COLUMNS[Tables.RECORD_DETAILS].ID.value
    passed_filter = COLUMNS[Tables.RECORD].PASSED_FILTER.value
    filter_failed = COLUMNS[Tables.RECORD].FILTER_FAILED.value
    waiting_since = COLUMNS[Tables.RECORD].WAITING_SINCE.value
    with get_engine().connect() as conn:
        records = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT {record}, {passed_filter} FROM {Tables.RECORD.value} WHERE "
                f"{passed_filter} IS NULL"
            )),
            con=conn
        )

    if len(records) == 0:
        return
    else:
        logger.info(f"Found {len(records)} records awaiting filtering.")

    # Check records against filters
    with get_engine().connect() as conn:
        records = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT * FROM {Tables.RECORD_DETAILS.value} WHERE "
                f"{accession_fk} IN "
                f"{tuple(records[record])}"
            )),
            con=conn
        )

    filters.apply_filters(records=records, col_name_passed=passed_filter, col_name_failed=filter_failed)

    # Save results
    # rename record number
    new_records = records[[accession_fk, passed_filter, filter_failed]]
    with Session(get_engine()) as session:
        session.execute(
            sqlalchemy.text((
                f"UPDATE {Tables.RECORD.value} "
                f"SET "
                f"{record}=:an, {passed_filter}=:fltr, {filter_failed}=:fail, {waiting_since} = NOW() "
                f"WHERE {record}=:an"
            )),
            [{'an': x[0], 'fltr': x[1], 'fail': x[2]} for x in new_records.itertuples(index=False)]
        )
        session.commit()
        # Let the api know the records are waiting
        session.execute(
            sqlalchemy.text((
                f"UPDATE {Tables.RECORD.value} "
                f"SET {waiting_since} = NOW() "
                f" WHERE {passed_filter} = True AND {waiting_since} IS NULL"
            ))
        )
        session.commit()

    if len(records) > 0:
        logger.info(f"{len(records.loc[records[passed_filter]])}/{len(records)} new records acceptable for assembly.")


def release_records() -> None:
    """
    When the web API is called up to request a new record record to assemble the
    record is marked as 'under consideration'.
    The requester should confirm their request within the time specified in the CONSIDERATION_PERIOD envvars,
    otherwise the record will be made available to other requesters for assembly.
    This function resets the stale records to make them available again.

    Additionally, any records whose assembly results have not been reported after
    the time specified in the ASSEMBLY_PERIOD envvars are marked as available again.
    """
    with Session(get_engine()) as session:
        session.execute(
            sqlalchemy.text((
                f"UPDATE {Tables.RECORD.value} "
                f"SET {COLUMNS[Tables.RECORD].ASSEMBLY_RESULT.value} = NULL, "
                f"{COLUMNS[Tables.RECORD].WAITING_SINCE.value} = NOW() "
                f"WHERE {COLUMNS[Tables.RECORD].ASSEMBLY_RESULT.value} = 'under consideration' "
                f"AND date_part("
                f"  '{Settings.CONSIDERATION_PERIOD_UNITS.value}', "
                f"  NOW() - {COLUMNS[Tables.RECORD].WAITING_SINCE.value}"
                f") >= {Settings.CONSIDERATION_PERIOD_N.value}"
            ))
        )
        session.execute(
            sqlalchemy.text((
                f"UPDATE {Tables.RECORD.value} "
                f"SET {COLUMNS[Tables.RECORD].ASSEMBLY_RESULT.value} = NULL, "
                f"{COLUMNS[Tables.RECORD].WAITING_SINCE.value} = NOW() "
                f"WHERE {COLUMNS[Tables.RECORD].ASSEMBLY_RESULT.value} = 'in progress' "
                f"AND date_part("
                f"  '{Settings.ASSEMBLY_PERIOD_UNITS.value}', "
                f"  NOW() - {COLUMNS[Tables.RECORD].WAITING_SINCE.value}"
                f") >= {Settings.ASSEMBLY_PERIOD_N.value}"
            ))
        )
        session.commit()


if __name__ == '__main__':
    """
    Run the next job in the queue and return the amount of time to sleep after completing.
    """
    while True:
        try:
            # Update taxon records
            taxon_ids = get_taxons_to_check()
            update_taxons(taxon_ids)

            # Release records that were requested but not acknowledged
            release_records()

        except BaseException as e:
            logger.error(e)

        sleep(30)
