import logging
import pandas
import datetime
import math
import pytz
import sqlalchemy
import os

from typing import Callable
from sqlalchemy.orm import Session
from time import sleep
from requests import request

from taxon_tracker.settings import Settings
from taxon_tracker.logging import DatabaseHandler, stream_handler, log_format
from taxon_tracker.database import Tables, COLUMNS, get_engine, AssemblyStatus


logger: [logging.Logger] = logging.getLogger(__file__)
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)
logger.addHandler(DatabaseHandler())
logger.setLevel(logging.DEBUG)

MAPBOX_KEY = os.environ.get('MAPBOX_KEY')
MAPBOX_URL = 'https://api.mapbox.com/geocoding/v5/mapbox.places-permanent/'


class ENAError(BaseException):
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
    # Fetch records for outdated taxons
    with get_engine().connect() as conn:
        df = pandas.read_sql(
            sql=sqlalchemy.text((
                f"SELECT {taxon_id}, {last_updated} FROM {Tables.TAXON.value} WHERE "
                f"{last_updated} IS NULL OR "
                # Updated over a week ago
                f"DATE_PART('{Settings.TAXON_UPDATE_UNITS.value}', NOW() - {last_updated}) >= "
                f"{Settings.TAXON_UPDATE_N.value}"
            )),
            con=conn
        )
    return df


def update_taxons(taxons: pandas.DataFrame) -> None:
    if len(taxons) == 0:
        logger.debug("No taxon ids need checking.")
        return

    t_id = COLUMNS[Tables.TAXON].ID.value
    last_updated = COLUMNS[Tables.TAXON].LAST_UPDATED.value
    for _, taxon in taxons.iterrows():
        update_records(taxon)
        # mark id as updated
        with Session(get_engine()) as session:
            session.execute(sqlalchemy.text((
                f"UPDATE {Tables.TAXON.value} SET {last_updated}=NOW() WHERE {t_id}={taxon[t_id]}"
            )))
            session.commit()


def update_records(taxon: pandas.Series) -> None:
    logger.info(f"Updating records for taxon id {taxon[COLUMNS[Tables.TAXON].ID.value]}.")
    # Query ENA for all record numbers
    query_ENA(taxon)


def query_ENA(taxon: pandas.Series) -> None:
    taxon_id = taxon[COLUMNS[Tables.TAXON].ID.value]
    last_updated = taxon[COLUMNS[Tables.TAXON].LAST_UPDATED.value]
    logger.debug(f"Last updated: {last_updated}")
    if pandas.isnull(last_updated):
        updated = Settings.EPOCH_DATE.value
    else:
        updated = last_updated.date().isoformat()
    logger.info(f"Fetching ENA records for taxon id {taxon_id} published since {updated}.")
    records = None
    offset = 0
    limit = Settings.ENA_REQUEST_LIMIT.value
    while True:
        url = (
            f"https://www.ebi.ac.uk/ena/portal/api/search?"
            f"format=json"
            f"&result=read_run"
            f"&fields=sample_accession,experiment_accession,run_accession"
            f",lat,lon,country,collection_date,fastq_ftp,first_public"
            # Core query
            f"&query=first_public%3E%3D{updated}"
            f"%20AND%20tax_eq({taxon_id})"
            # Filters
            f"%20AND%20library_strategy=WGS"
            f"%20AND%20instrument_platform=ILLUMINA"
            f"%20AND%20library_source=GENOMIC"
            f"%20AND%20library_layout=PAIRED"
            f"%20AND%20base_count%3E%3D{int(os.environ.get('MIN_BASE_PAIRS', '0'))}"
            # Pagination
            f"&limit={limit}"
            f"&offset={offset}"
        )
        logger.debug(url)
        result = request('GET', url)

        if result.status_code == 204:
            # Undocumented, but ENA sends 204 when asking for out-of-range results
            logger.debug("End of results.")
            break
        if result.status_code != 200:
            raise ENAError(result.text)

        df = pandas.read_json(result.text)
        logger.debug(f"Fetched {len(df)} records.")

        if type(df) is not pandas.DataFrame:
            break
        if isinstance(records, pandas.DataFrame):
            records = pandas.concat([df, records], ignore_index=True)
        else:
            records = df

        if len(df) >= limit:
            offset = offset + limit
        else:
            break

    if not isinstance(records, pandas.DataFrame):
        logger.info(f"No records found that pass filters.")
        return

    logger.info(f"Found {len(records)} records that pass filters.")

    t_id = COLUMNS[Tables.RECORD].TAXON.value
    r_id = COLUMNS[Tables.RECORD].ID.value
    # Add in record id
    record_ids = []
    for r in range(len(records)):
        row = records.iloc[r].to_dict()
        record_ids.append((
            f"{row[COLUMNS[Tables.RECORD].SAMPLE_ACCESSION.value]}_"
            f"{row[COLUMNS[Tables.RECORD].EXPERIMENT_ACCESSION.value]}_"
            f"{row[COLUMNS[Tables.RECORD].RUN_ACCESSION.value]}"
        ))
    records[r_id] = record_ids
    records[COLUMNS[Tables.RECORD].TAXON.value] = taxon_id
    records[COLUMNS[Tables.RECORD].ASSEMBLY_RESULT.value] = AssemblyStatus.WAITING.value

    with get_engine().connect() as conn:
        existing_records = pandas.read_sql(sqlalchemy.text((
            f"SELECT {r_id} FROM {Tables.RECORD.value} WHERE {t_id} = {taxon_id}"
        )), con=conn)

    new_records = records.loc[~records[r_id].isin(existing_records[r_id])]

    logger.info(f"{len(new_records)} records are new, {len(records) - len(new_records)} exist locally.")

    # Fetch records if they don't already exist.
    if len(new_records) > 0:
        try:
            new_records = mapbox_country_lookup(new_records)
            new_records[COLUMNS[Tables.RECORD].TIME_FETCHED.value] = datetime.datetime.now(tz=pytz.UTC)

            with get_engine().connect() as conn:
                new_records.to_sql(
                    name=Tables.RECORD.value,
                    con=conn,
                    if_exists='append',
                    index=False
                )
                conn.commit()
                logger.info(f"Saved {len(new_records)} new records.")

        except BaseException as e:
            logger.error((
                f"Error saving ENA record details. They will be retrieved later. Error: {e}"
            ))


def mapbox_country_lookup(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Insert lat/lon estimations for records by consulting Mapbox if necessary.
    """
    # Select entries with no lat or lon but with country
    logger.debug('Mapbox lookup')
    logger.debug('SKIPPED')
    df[COLUMNS[Tables.RECORD].PASSED_FILTER.value] = True
    df[COLUMNS[Tables.RECORD].LAT_LON_INTERPOLATED.value] = False
    return df

    subset = df.loc[
        (df[COLUMNS[Tables.RECORD].LATITUDE.value].isna() |
         df[COLUMNS[Tables.RECORD].LONGITUDE.value].isna()) &
        df[COLUMNS[Tables.RECORD].COUNTRY.value].notna(), ]
    logger.debug(f"{len(subset)} rows require interpolation")

    # Load country -> lat/lon mappings from database
    logger.debug('Loading Mapbox cache from database')
    with get_engine().connect() as conn:
        mapbox_lookup_table = pandas.read_sql(
            sql=sqlalchemy.text(f"SELECT * FROM {Tables.COUNTRY_COORDINATES.value}"),
            con=conn
        )
    logger.debug(f'Loaded {len(mapbox_lookup_table)} rows from database')

    # Column name aliases
    cc_country = COLUMNS[Tables.COUNTRY_COORDINATES].COUNTRY.value
    cc_lat = COLUMNS[Tables.COUNTRY_COORDINATES].LATITUDE.value
    cc_lon = COLUMNS[Tables.COUNTRY_COORDINATES].LONGITUDE.value
    rd_country = COLUMNS[Tables.RECORD].COUNTRY.value
    rd_lat = COLUMNS[Tables.RECORD].LATITUDE.value
    rd_lon = COLUMNS[Tables.RECORD].LONGITUDE.value
    rd_interp = COLUMNS[Tables.RECORD].LAT_LON_INTERPOLATED.value

    for c in subset.country.unique():
        logger.debug(f'Mapbox processing for {c}')
        lat = None
        lon = None
        # Fetch new lat/lon mappings where necessary
        if c not in mapbox_lookup_table[cc_country]:
            try:
                logger.debug(f"Querying mapbox for {c}")
                response = request(
                    'GET',
                    f"{MAPBOX_URL}/{c}.json?access_token={MAPBOX_KEY}"
                )
                j = response.json()
                results = j['features']
                top_result = results[0]
                lat, lon = top_result['center']
                mapbox_lookup_table = mapbox_lookup_table.concat(
                    [
                        mapbox_lookup_table,
                        pandas.DataFrame.from_dict({
                            cc_country: c,
                            cc_lat: lat,
                            cc_lon: lon
                        })
                    ],
                    ignore_index=True
                )
            except BaseException as e:
                # Soft fail on error
                logger.error(e)
            else:
                # Load from database cache
                logger.debug(f"Found {c} in database")
                row = mapbox_lookup_table[mapbox_lookup_table[cc_country] == c]
                lat = row[cc_lat][0]
                lon = row[cc_lon][0]

            # Update records
            if lat is not None and lon is not None:
                logger.debug(f"{c} resolves to {lat}, {lon}")
                subset.loc[subset[rd_country] == c, [rd_lat, rd_lon, rd_interp]] = (lat, lon, True)

    # Save updated lookup table
    logger.debug("Saving updated records.")
    with get_engine().connect() as conn:
        mapbox_lookup_table.to_sql(
            name=Tables.COUNTRY_COORDINATES.value,
            con=conn,
            if_exists='append',
            index=False
        )
        conn.commit()

    logger.debug("Interpolation complete")
    return df


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
                f"SET {COLUMNS[Tables.RECORD].ASSEMBLY_RESULT.value} = '{AssemblyStatus.WAITING}', "
                f"{COLUMNS[Tables.RECORD].WAITING_SINCE.value} = NOW() "
                f"WHERE {COLUMNS[Tables.RECORD].ASSEMBLY_RESULT.value} = '{AssemblyStatus.IN_PROGRESS}' "
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
