import pandas
import os
import pandas as pd
import numpy as np
import pickle
# import mapbox
import sys
from html import unescape
from typing import Callable
import logging

logger = logging.getLogger(__file__)

MABPOX_URL = "https://data-flo.io/api/dataflows/run/kvpsi3T8V"
MABPOX_KEY = os.environ.get('MABPOX_KEY')


class FilterImplementationException(Exception):
    pass


class Filter:
    def __init__(self, name: str, lambda_fun: Callable[[pandas.DataFrame], pandas.Series]):
        self.name = name
        self._fun = lambda_fun

    def do(self, df: pandas.DataFrame, col_names: list) -> pandas.DataFrame:
        """
        Set two columns on df:
        col_names[0] is set by applying the Filter's function to df
        col_names[1] is set as the Filter's name if the filter is failed, otherwise to the empty string
        """
        df[col_names[0]] = self._fun(df)
        df[col_names[1]] = np.where(df[col_names[0]], '', self.name)
        return df


class FilterNA(Filter):
    def __init__(self, name: str, field: str):
        def lambda_fun(df: pandas.DataFrame) -> pandas.Series:
            return df[field].notna()
        super(FilterNA, self).__init__(name=name, lambda_fun=lambda_fun)


class FilterMatch(Filter):
    def __init__(self, name: str, field: str, value: any):
        if type(value) is not list:
            value = [value]

        def lambda_fun(df: pandas.DataFrame) -> pandas.Series:
            return df[field].isin(value)

        super(FilterMatch, self).__init__(name=name, lambda_fun=lambda_fun)


def f_genome_size(df: pandas.DataFrame) -> pandas.Series:
    size = os.environ.get('FILTER_GENOME_SIZE', None)
    if size is None:
        raise FilterImplementationException('Environment variable FILTER_GENOME_SIZE is not defined.')
    depth = os.environ.get('FILTER_X_DEPTH', None)
    if depth is None:
        raise FilterImplementationException('Environment variable FILTER_X_DEPTH is not defined.')
    return df.genome_size >= int(size) * int(depth)


def f_location_or_date(df: pandas.DataFrame) -> pandas.Series:
    # 1. Filter out entries with no lat nor lon 17,655/116,246
    has_lat_lon_df = df[df["lon"].notna()&df["lat"].notna()]
    # 2. Select entries with no lat or lon 98,591/116,246
    no_lat_lon_df = df[df["lon"].isna()|df["lat"].isna()]
    # 3. Select entries without lat or lon but with country 93,058/98,591
    has_country_df = no_lat_lon_df[no_lat_lon_df["country"].notna()]
    no_country_no_ll_df = no_lat_lon_df[no_lat_lon_df["country"].isna()]
    # 4. Load 'location' dictionary from pickle
    with open('locations.pkl', 'rb') as handle:
        locations = pickle.load(handle)

    # 5. Go through 'country' values and find their geo coordinates if
    #    not in 'location' dictionary.
    # for i in has_country_df.country.unique():
    #     if i not in locations:
    #         locations[i] = mapbox.get_lat_lon(MABPOX_URL, unescape(i), MABPOX_KEY)

    # 6. Save updated 'location' to pickle
    with open('locations.pkl', 'wb') as handle:
        pickle.dump(locations, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # 7. Prepare to map geo coordinates back to
    lat = {k: v[0] for k, v in locations.items()}
    lon = {k: v[1] for k, v in locations.items()}

    # 8. Map geo coordinates to pandas dataframe
    has_country_df = has_country_df.copy()
    has_country_df['lat'] = has_country_df['country'].map(lat)
    has_country_df['lon'] = has_country_df['country'].map(lon)

    # 9. Concatenate entries with lat_lon and with country
    final_df = pd.concat([has_country_df, has_lat_lon_df, no_country_no_ll_df])

    # Select rows with at least either location or date
    return df in final_df[~(final_df["collection_date"].isna() & (final_df["lat"].isna() & final_df["lon"].isna()))]


FILTERS = [
    FilterMatch('library_strategy=WGS', 'library_strategy', 'WGS'),
    FilterMatch('instrument_platform=ILLUMINA', 'instrument_platform', 'ILLUMINA'),
    FilterMatch('library_source=GENOMIC', 'library_source', 'GENOMIC'),
    FilterMatch('library_layout=PAIRED', 'library_layout', 'PAIRED'),
    Filter('base_count size', f_genome_size),
    FilterMatch('date acceptable', 'collection_date', ["1000-01-01","1800-01-01"]),
    # Filter('location or date', f_location_or_date)
]


def apply_filters(
        records: pandas.DataFrame,
        col_name_passed: str = 'passed_filter',
        col_name_failed: str = 'filter_failed'
) -> pandas.DataFrame:
    """
    Apply all filters to a record set.
    Will return a record set with col_name_passed and col_name_failed added.
    col_name_passed will be True if all filters were passed, False otherwise.
    col_name_failed will be the name of the first failed filter, or the empty string
    """
    # Drop non-WGS experiments
    records[col_name_passed] = False
    okay = records
    for f in FILTERS:
        try:
            okay = f.do(df=okay, col_names=[col_name_passed, col_name_failed])
            okay = okay.loc[okay[col_name_passed]]
            if len(okay) == 0:
                break
        except FilterImplementationException as e:
            logger.warning(f"Failed to implement filter {f.name}: {e}")

    return records
