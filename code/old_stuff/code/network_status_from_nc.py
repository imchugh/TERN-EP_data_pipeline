#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 08:52:15 2025

@author: imchugh
"""

import datetime as dt
import pandas as pd
import time

from file_handling import nc_io, file_handler
from data_constructors.convert_calc_filter import filter_range
from managers import metadata, paths

DUMMY_DICT = {'logger': 'N/A', 'table': 'N/A'}

class SiteStatusParser():

    #--------------------------------------------------------------------------
    def __init__(self, site):

        self.site = site

        # Get and edit the metadata manager to reflect the fact that variances
        # are converted during netcdf production
        md_mngr = metadata.MetaDataManager(site=site)
        renamer = {
            var: var.replace('Vr', 'Sd')
            for var in md_mngr.list_variance_variables()
            }
        md_mngr.site_variables.loc[renamer.keys(), 'statistic_type'] = (
            'standard_deviation'
            )
        md_mngr.site_variables.loc[renamer.keys(), 'process'] = (
            'Sd'
            )
        md_mngr.site_variables = md_mngr.site_variables.rename(renamer)
        self.md_mngr = md_mngr
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_status(self, run_time: dt.datetime=None) -> pd.DataFrame:
        """
        Read netcdf file to get data status.

        Args:
            run_time (optional): time to use for comparison of data age.
            Defaults to None.

        Returns:
            dataframe containing results.

        """

        # Grab the site-adjusted run time
        run_time = _get_site_time(
            site=self.site,
            UTC_offset=self.md_mngr.site_details['UTC_offset'],
            run_time=run_time
            )

        # Get and read the data, then rename it to erase the system-specific
        # turbulent flux names
        file = sorted(
            paths.get_local_stream_path(
                resource='homogenised_data',
                stream='nc',
                subdirs=[self.site]
                )
            .glob('*.nc')
            )[-1]
        reader = nc_io.NCReader(nc_file=file)

        # Create a dataframe and parse the variables inidividually
        df = reader.get_dataframe()
        rslt = []
        for variable in self.md_mngr.list_variables():
            attrs = self.md_mngr.get_variable_attributes(variable=variable)
            try:
                sub_attrs = attrs[['logger', 'table', 'file']].to_dict()
            except KeyError:
                sub_attrs = DUMMY_DICT | {'file': attrs['file']}

            rslt.append(
                sub_attrs |
                _parse_variable(
                    variable=df[variable],
                    var_range=(attrs.plausible_min, attrs.plausible_max)
                    )
                )

        # Dump to df and return
        return (
            pd.DataFrame(
                data=rslt,
                index=self.md_mngr.list_variables())
            .rename_axis('variable')
            .reset_index()
            )
    #--------------------------------------------------------------------------

    #------------------------------------------------------------------------------
    def get_file_status(self, run_time: dt.datetime=None) -> pd.DataFrame:
        """
        Get the file attributes of each file mapped in the metadata manager.

        Args:
            run_time (optional): time to use for comparison of data age.
            Defaults to None.

        Returns:
            Dataframe containing the info for all listed files.

        """

        # Grab the site-adjusted run time
        site_time = _get_site_time(
            site=self.site,
            UTC_offset=self.md_mngr.site_details['UTC_offset'],
            run_time=run_time
            )

        # Get the logger / file particulars (currently must handle sites with no
        # logger or table)
        try:
            files_df = (
                self.md_mngr.site_variables[['logger', 'table', 'file']]
                .drop_duplicates()
                .reset_index(drop=True)
                .set_index(keys='file')
                )
        except KeyError:
            files_df = (
                self.md_mngr.site_variables[['file']]
                .drop_duplicates()
                .reset_index(drop=True)
                .set_index(keys='file')
                )

        # Get the file attributes
        attrs_df = (
            pd.concat(
                [
                    self.md_mngr.get_file_attributes(x)
                    for x in self.md_mngr.list_files()
                    ],
                axis=1
                )
            .T
            .drop(['station_name', 'table_name'], axis=1)
            )
        attrs_df.index = self.md_mngr.list_files()

        # Get the percentage missing data for each file
        # Note that here, we DONT try to concatenate if the file is an EP master
        # file because the concatenation is already handled
        missing_list = []
        do_concat = False
        for file in self.md_mngr.list_files(abs_path=True):
            do_concat = True
            if attrs_df.loc[file.name, 'format'] != 'TOA5':
                do_concat = False
            missing_list.append(
                file_handler.DataHandler(file=file, concat_files=do_concat)
                .get_missing_records()
                )
        missing_df = pd.DataFrame(
            data=missing_list, index=self.md_mngr.list_files()
            )

        # Combine all
        combined_df = pd.concat([files_df, attrs_df, missing_df], axis=1)
        combined_df.index.name = 'file'
        output_df = (
            combined_df
            .assign(
                days_since_last_record = (
                    (site_time - combined_df.end_date).apply(lambda x: x.days)
                    )
                )
            .assign(site=self.site)
            .reset_index()
            )

        try:
            return output_df.set_index(keys=['site', 'logger', 'table'])
        except KeyError:
            return output_df.set_index(keys='file')
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_variable(variable: pd.Series, var_range: tuple) -> dict:
    """


    Args:
        variable (pd.Series): DESCRIPTION.
        var_range (tuple): DESCRIPTION.

    Returns:
        dict: DESCRIPTION.

    """

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
