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

from backend import fetch_accession_numbers

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
    taxon_id = COLUMNS[Tables.TAXON].TAXON_ID.value
    last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
    # Fetch accessions for outdated taxon_ids
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

    t_id = COLUMNS[Tables.TAXON].TAXON_ID.value
    last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
    for taxon_id in taxon_ids.taxon_id:
        update_records(taxon_id)
        # mark taxon_id as updated
        with Session(get_engine()) as session:
            session.execute(sqlalchemy.text((
                f"UPDATE {Tables.TAXON.value} SET {last_updated}=NOW() WHERE {t_id}={taxon_id}"
            )))
            session.commit()


def update_records(taxon_id: int) -> None:
    logger.info(f"Updating records for taxon id {taxon_id}.")
    # Query ENA for all accession numbers
    query_ENA(taxon_id)

    # Filter new records for suitability
    filter_accession_numbers()


def query_ENA(taxon_id: int, all_accessions=None) -> None:
    logger.info(f"Fetching ENA accession numbers for taxon id {taxon_id}.")
    accessions = None
    offset = 0
    limit = Settings.ENA_REQUEST_LIMIT.value
    while True:
        url = (
            f"https://www.ebi.ac.uk/ena/portal/api/links/taxon?"
            f"accession={taxon_id}"
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
        if accessions is None:
            accessions = df
        else:
            accessions = pandas.concat([df, accessions], ignore_index=True)

        if len(df) >= limit:
            offset = offset + limit
        else:
            break

    logger.debug(f"Found {len(accessions)} accession numbers for taxon_id {taxon_id}.")

    t_id = COLUMNS[Tables.ACCESSION].TAXON_ID.value
    run_accession = COLUMNS[Tables.ACCESSION].RUN_ACCESSION.value

    with get_engine().connect() as conn:
        existing_records = pandas.read_sql(sqlalchemy.text((
            f"SELECT {run_accession} FROM {Tables.ACCESSION.value} WHERE {t_id} = {taxon_id}"
        )), con=conn)

    missing = accessions.loc[~accessions[run_accession].isin(existing_records[run_accession])]

    logger.info(f"{len(existing_records)}/{len(accessions)} ENA records exist locally.")

    # Fetch records if they don't already exist.
    if len(missing) > 0:
        fetch_ENA_records(missing, taxon_id)


def fetch_ENA_records(accessions: pandas.DataFrame, taxon_id: int) -> None:
    logger.info(f"Fetching records for {len(accessions)} accessions.")
    limit = Settings.ENA_REQUEST_LIMIT.value
    response_limit = 0
    successes = []
    for i in range(math.ceil(len(accessions) / limit)):
        ans = accessions.iloc[i * limit:(i + 1) * limit]
        url = "https://www.ebi.ac.uk/ena/portal/api/search"
        data = {
            'includeAccessions': f"{','.join(ans[COLUMNS[Tables.ACCESSION].RUN_ACCESSION.value])}",
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
                accession_ids = []
                for r in range(len(records)):
                    row = records.iloc[r].to_dict()
                    accession_ids.append((
                        f"{row[COLUMNS[Tables.RECORD_DETAILS].SAMPLE_ACCESSION.value]}_"
                        f"{row[COLUMNS[Tables.RECORD_DETAILS].EXPERIMENT_ACCESSION.value]}_"
                        f"{row[COLUMNS[Tables.RECORD_DETAILS].RUN_ACCESSION.value]}"
                    ))
                records[COLUMNS[Tables.RECORD_DETAILS].ACCESSION_ID.value] = accession_ids
                records[COLUMNS[Tables.RECORD_DETAILS].TIME_FETCHED.value] = datetime.datetime.now(tz=pytz.UTC)

                # Slim table for saving space
                slim_records = records.filter(items=[
                    COLUMNS[Tables.RECORD_DETAILS].ACCESSION_ID.value,
                    COLUMNS[Tables.RECORD_DETAILS].SAMPLE_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].RUN_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].EXPERIMENT_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].TIME_FETCHED.value
                ])
                slim_records = slim_records.copy()
                slim_records = slim_records.rename(columns={
                    COLUMNS[Tables.RECORD_DETAILS].ACCESSION_ID.value:
                        COLUMNS[Tables.ACCESSION].ACCESSION_ID.value,
                    COLUMNS[Tables.RECORD_DETAILS].SAMPLE_ACCESSION.value:
                        COLUMNS[Tables.ACCESSION].SAMPLE_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].RUN_ACCESSION.value:
                        COLUMNS[Tables.ACCESSION].RUN_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].EXPERIMENT_ACCESSION.value:
                        COLUMNS[Tables.ACCESSION].EXPERIMENT_ACCESSION.value,
                    COLUMNS[Tables.RECORD_DETAILS].TIME_FETCHED.value:
                        COLUMNS[Tables.ACCESSION].TIME_FETCHED.value
                })
                slim_records[COLUMNS[Tables.ACCESSION].TAXON_ID.value] = taxon_id

                with get_engine().connect() as conn:
                    records.to_sql(
                        name=Tables.RECORD_DETAILS.value,
                        con=conn,
                        if_exists='append',
                        index=False
                    )
                    slim_records.to_sql(
                        name=Tables.ACCESSION.value,
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

    logger.info(f"Fetched {len(successes)}/{len(accessions)} record details.")


def filter_accession_numbers() -> None:
    """
    Fetch records for any accession numbers without a passed_filter decision and apply filters.
    """
    accession = COLUMNS[Tables.ACCESSION].ACCESSION_ID.value
    accession_fk = COLUMNS[Tables.RECORD_DETAILS].ACCESSION_ID.value
    passed_filter = COLUMNS[Tables.ACCESSION].PASSED_FILTER.value
    filter_failed = COLUMNS[Tables.ACCESSION].FILTER_FAILED.value
    waiting_since = COLUMNS[Tables.ACCESSION].WAITING_SINCE.value
    with get_engine().connect() as conn:
        accessions = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT {accession}, {passed_filter} FROM {Tables.ACCESSION.value} WHERE "
                f"{passed_filter} IS NULL"
            )),
            con=conn
        )

    if len(accessions) == 0:
        return
    else:
        logger.info(f"Found {len(accessions)} records awaiting filtering.")

    # Check records against filters
    with get_engine().connect() as conn:
        records = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT * FROM {Tables.RECORD_DETAILS.value} WHERE "
                f"{accession_fk} IN "
                f"{tuple(accessions[accession])}"
            )),
            con=conn
        )

    filters.apply_filters(records=records, col_name_passed=passed_filter, col_name_failed=filter_failed)

    # Save results
    # rename accession number
    new_accessions = records[[accession_fk, passed_filter, filter_failed]]
    with Session(get_engine()) as session:
        session.execute(
            sqlalchemy.text((
                f"UPDATE {Tables.ACCESSION.value} "
                f"SET "
                f"{accession}=:an, {passed_filter}=:fltr, {filter_failed}=:fail "
                f"WHERE {accession}=:an"
            )),
            [{'an': x[0], 'fltr': x[1], 'fail': x[2]} for x in new_accessions.itertuples(index=False)]
        )
        session.commit()
        # Let the droplet manager know the records are waiting
        session.execute(
            sqlalchemy.text((
                f"UPDATE {Tables.ACCESSION.value} "
                f"SET {waiting_since} = now() WHERE "
                f"{passed_filter} = True AND {waiting_since} IS NULL"
            ))
        )
        session.commit()

    if len(records) > 0:
        logger.info(f"{len(records.loc[records[passed_filter]])}/{len(records)} new records acceptable for assembly.")


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
            with get_engine().connect() as conn:
                droplets = pandas.read_sql(sqlalchemy.text((
                    f"SELECT id FROM {Tables.DROPLETS.value} "
                    f"WHERE {COLUMNS[Tables.DROPLETS].COMPLETE.value} = True"
                )), con=conn)
            if len(droplets) >= Settings.MAX_DROPLETS.value:
                logger.debug(f"Number of active droplets ({len(droplets)} already at maximum.")
                continue
            else:
                logger.debug(f"{len(droplets)}/{Settings.MAX_DROPLETS.value} droplets active.")

            # Check for accession to launch
            with get_engine().connect() as conn:
                record = pandas.read_sql(sqlalchemy.text((
                    f"SELECT "
                    f"{COLUMNS[Tables.ACCESSION].ACCESSION_ID.value},"
                    f"{COLUMNS[Tables.ACCESSION].WAITING_SINCE.value} "
                    f"FROM "
                    f"{Tables.ACCESSION.value} WHERE "
                    f"{COLUMNS[Tables.ACCESSION].WAITING_SINCE.value} IS NOT NULL "
                    f"ORDER BY {COLUMNS[Tables.ACCESSION].WAITING_SINCE.value}  "
                    f"LIMIT 1"
                )), con=conn)
            if len(record) <= 0:
                logger.debug(f"No records are awaiting assembly.")
                continue
            else:
                logger.info(f"Assembling {record.loc[0, COLUMNS[Tables.ACCESSION].ACCESSION_ID.value]}.")

            # Launch assembler


            # Find the next job

        except BaseException as e:
            logger.error(e)

        sleep(30)
