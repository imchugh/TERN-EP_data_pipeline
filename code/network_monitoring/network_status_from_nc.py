#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 08:52:15 2025

@author: imchugh
"""

import datetime as dt
import pandas as pd
import time

from file_handling import nc_reader, file_handler
from data_constructors.convert_calc_filter import filter_range
from managers import metadata, paths

def get_data_status(site: str, run_time: dt.datetime=None) -> pd.DataFrame:
    """
    Read netcdf file to get data status.

    Args:
        site: name of site.
        run_time (optional): time to use for comparison of data age. Defaults to None.

    Returns:
        TYPE: DESCRIPTION.

    """

    # Get the metadata manager and rename the system-specific turbulent flux
    # names to standard names.
    md_mngr = metadata.MetaDataManager(site=site)
    flux_map = md_mngr.map_fluxes_to_standard_names()
    md_mngr.site_variables = md_mngr.site_variables.rename(flux_map)

    # Grab the site-adjusted run time
    run_time = _get_site_time(
        site=site,
        UTC_offset=md_mngr.get_site_details(field='UTC_offset'),
        run_time=run_time
        )

    # Get and read the data, then rename it to erase the system-specific
    # turbulent flux names
    file = sorted(
        paths.get_local_stream_path(
            resource='homogenised_data',
            stream='nc',
            subdirs=[site]
            )
        .glob('*.nc')
        )[-1]
    reader = nc_reader.NCReader(nc_file=file)
    reader.ds = reader.ds.rename(flux_map)

    # Create a dataframe and parse the variables inidividually
    df = reader.get_dataframe()
    rslt = []
    for variable in md_mngr.list_variables():
        attrs = md_mngr.get_variable_attributes(variable=variable)
        rslt.append(
            attrs[['logger', 'table', 'file']].to_dict() |
            _parse_variable(
                variable=df[variable],
                var_range=(attrs.plausible_min, attrs.plausible_max)
                )
            )

    # Dump to df and return
    return (
        pd.DataFrame(
            data=rslt,
            index=md_mngr.list_variables())
        .rename_axis('variable')
        .reset_index()
        )

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_status(
        site: str, md_mngr: metadata.MetaDataManager=None,
        run_time: dt.datetime=None
        ) -> pd.DataFrame:
    """
    Get the file attributes of each file mapped in the metadata manager.

    Args:
        site: name of site.
        md_mngr: metadata manager from which to draw file info. Defaults to None.

    Returns:
        Dataframe containing the info for all listed files.

    """

    # Inits
    if md_mngr is None:
        md_mngr = metadata.MetaDataManager(site=site)

    # Grab the site-adjusted run time
    site_time = _get_site_time(
        site=site,
        UTC_offset=md_mngr.get_site_details(field='UTC_offset'),
        run_time=run_time
        )

    # Get the logger / file particulars (currently must handle sites with no
    # logger or table)
    try:
        files_df = (
            md_mngr.site_variables[['logger', 'table', 'file']]
            .drop_duplicates()
            .reset_index(drop=True)
            )
    except KeyError:
        files_df = (
            md_mngr.site_variables[['file']]
            .assign(logger='', table='')
            [['logger','table','file']]
            .reset_index(drop=True)
            )

    # Get the file attributes
    attrs_df = (
        pd.concat(
            [md_mngr.get_file_attributes(x) for x in files_df.file],
            axis=1
            )
        .T
        .drop(['station_name', 'table_name'], axis=1)
        )

    # Get the percentage missing data for each file
    # Note that here, we DONT try to concatenate if the file is an EP master
    # file because the concatenation is already handled
    missing_list = []
    do_concat = False
    for file in md_mngr.list_files(abs_path=True):
        if md_mngr.get_file_attributes(file.name)['format'] == 'TOA5':
            do_concat = True
        else:
            do_concat = False
        missing_list.append(
            file_handler.DataHandler(file=file, concat_files=do_concat)
            .get_missing_records()
            )
    missing_df = pd.DataFrame(missing_list)

    # Combine all and return
    combined_df = pd.concat([files_df, attrs_df, missing_df], axis=1)
    return (
        combined_df
        .assign(
            days_since_last_record = (
                (site_time - combined_df.end_date).apply(lambda x: x.days)
                )
            )
        .assign(site=site)
        .set_index(keys=['site', 'logger', 'table'])
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_variable(variable: pd.Series, var_range: tuple) -> dict:

    # If the variable has no valid data, use the dummy outputs
    # Otherwise, calculate the last valid values etc.
    filtered_variable = (
        filter_range(
            series=variable.copy(),
            min_val=var_range[0], max_val=var_range[1]
            )
        .dropna()
        )

    if len(filtered_variable) == 0:
        return {
            'last_valid_value': None,
            'last_valid_record_datetime': None,
            'last_24hr_pct_valid': 0,
            'days_since_last_valid_record': 'N/A'
            }
    else:
        now = dt.datetime.now()
        lvr = filtered_variable.iloc[-1]
        dt_lvr = filtered_variable.index[-1]
        how_old = (now - dt_lvr).days
        pct_valid = 0
        if not how_old > 0:
            pct_valid = round(
                len(filtered_variable.loc[now - dt.timedelta(days=1): now]) /
                len(variable.loc[now - dt.timedelta(days=1): now]) *
                100
                )
        return {
            'last_valid_value': lvr,
            'last_valid_record_datetime': dt_lvr,
            'last_24hr_pct_valid': pct_valid,
            'days_since_last_valid_record': how_old
            }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_site_time(site, UTC_offset, run_time=None):
    """
    Correct server time to site-based local standard time.

    Args:
        site: name of site.
        UTC_offset: difference in hours between UTC and local meridian.
        run_time (optional): the time to use for calculation of time since
            valid variables reported. If not supplied, system time at runtime is
            used. Defaults to None.

    Returns:
        TYPE: DESCRIPTION.

    """

    server_utc_offset = time.localtime().tm_gmtoff / 3600
    if run_time is None:
        run_time = dt.datetime.now()
    return run_time - dt.timedelta(hours=server_utc_offset - UTC_offset)
#------------------------------------------------------------------------------
