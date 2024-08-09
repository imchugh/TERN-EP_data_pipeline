# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 14:42:54 2024

@author: jcutern-imchugh
"""

import datetime as dt
import logging
import numpy as np
import os
import pandas as pd
import time

import file_handling.file_io as io
import file_handling.file_handler as fh
from utils.configs_manager import PathsManager
import utils.metadata_handlers as mh
from sparql_site_details import site_details

paths_mngr = PathsManager()
sd_mngr = site_details()

logger = logging.getLogger(__name__)

###############################################################################
### BEGIN MAIN FUNCTIONS ###
###############################################################################

def throw_error():

    raise RuntimeError('Shes a biggun!')


#------------------------------------------------------------------------------
def write_status_xlsx() -> None:
    """Evaluate status of all sites and write to xlsx. Returns None."""

    # Inits
    output_path = 'E:/Scratch/status_test.xlsx'
    site_list = [
        'AliceSpringsMulga', 'Boyagin', 'Calperum', 'Fletcherview', 'Gingin',
        'GreatWesternWoodlands', 'HowardSprings', 'Litchfield', 'MyallValeA',
        'MyallValeB', 'Ridgefield', 'SnowGum', 'SturtPlains', 'Wellington',
        'Yanco'
        ]
    run_time = dt.datetime.now()

    slow_file_status_list = []
    fast_file_status_list = []
    site_data_status_dict = {}

    logger.info('Evaluating site status: ')

    # Collate data using single call to metadata manager per site
    for site in site_list:

        logger.info(f'    - {site}')


        md_mngr = mh.MetaDataManager(site=site, variable_map='vis')
        slow_file_status_list.append(
            get_slow_file_status(site=site, md_mngr=md_mngr)
            )
        fast_file_status_list.append(
            {'site': site} |
            get_fast_file_status(site=site)
            )
        site_data_status_dict[site] = (
            get_slow_data_status(site=site, md_mngr=md_mngr)
            .reset_index()
            )

    # Write sheets
    with pd.ExcelWriter(path=output_path) as writer:

        # Write the slow file summary (all site) sheet
        _write_file_status(
            files_df=pd.concat(slow_file_status_list).reset_index(),
            writer=writer,
            sheet_name='Slow_file_summary',
            run_time=run_time
            )

        # Write the fast file summary (all site) sheet
        _write_file_status(
            files_df=pd.DataFrame(fast_file_status_list),
            writer=writer,
            sheet_name='Fast_file_summary',
            run_time=run_time
            )

        # Write the individual slow data site sheets
        for site in site_list:

            _write_site_data_status(
                site=site,
                data_df=site_data_status_dict[site],
                writer=writer,
                run_time=run_time
                )

        # Write key
        _write_key(writer=writer)

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_status_json():

    DATA_SUBSET = ['Fco2', 'Fe', 'Fh', 'Fsd']

    input_path = ''
    pass
#------------------------------------------------------------------------------

###############################################################################
### END MAIN FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN DATA COLLATION FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def get_slow_file_status(
        site: str, md_mngr: mh.MetaDataManager=None, run_time: dt.datetime=None
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
        md_mngr = mh.MetaDataManager(site=site, variable_map='vis')
    site_time = _get_site_time(site=site, run_time=run_time)

    # Get the logger / file particulars
    files_df = (
        md_mngr.site_variables[['logger', 'table', 'file']]
        .drop_duplicates()
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
    missing_df = pd.DataFrame(
        [
            fh.DataHandler(file=file, concat_files=True).get_missing_records()
            for file in md_mngr.list_files(abs_path=True)
            ]
        )

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
def get_slow_data_status(site: str, md_mngr=None, run_time=None) -> pd.DataFrame:
    """
    Get the status of variables mapped in the metadata manager.

    Args:
        site: name of site.
        md_mngr (optional): metadata manager from which to draw file info.
            Defaults to None.
        run_time (optional): the time to use for calculation of time since
            valid variables reported. If not supplied, system time at runtime is
            used. Defaults to None.

    Returns:
        Dataframe containing the status of all mapped variables.

    """

    # Inits
    if md_mngr is None:
        md_mngr = mh.MetaDataManager(site=site, variable_map='vis')
    run_time = _get_site_time(site=site, run_time=run_time)
    dummy_dict = {
        'last_valid_value': None,
        'last_valid_record_datetime': None,
        'last_24hr_pct_valid': 0,
        'days_since_last_valid_record': 'N/A'
        }

    data_df = (
        io.get_data(file=f'E:/Scratch/{site}_merged_std.dat')
        .drop('TIMESTAMP', axis=1)
        )
    l = []
    now = dt.datetime.now()
    for var in data_df.columns:

        # If the variable is constructed, it will not have any attributes
        try:
            attrs_dict = (
                md_mngr.get_variable_attributes(variable=var)
                [['logger', 'table', 'file']]
                .to_dict()
                )
        except KeyError:
            attrs_dict = {}

        # If the variable has no valid data, use the dummy outputs
        # Otherwise, calculate the last valid values etc.
        s = data_df[var].dropna()
        if len(s) == 0:
            attrs_dict.update(dummy_dict)
        else:
            lvr = s.iloc[-1]
            dt_lvr = s.index[-1]
            how_old = (now - dt_lvr).days
            pct_valid = 0
            if not how_old > 0:
                pct_valid = round(
                    len(s.loc[now - dt.timedelta(days=1): now]) /
                    len(data_df[var].loc[now - dt.timedelta(days=1): now]) *
                    100
                    )
            attrs_dict.update(
                {
                    'last_valid_value': lvr,
                    'last_valid_record_datetime': dt_lvr,
                    'last_24hr_pct_valid': pct_valid,
                    'days_since_last_valid_record': how_old
                    }
                )
        l.append(attrs_dict)

    return (
        pd.DataFrame(
            data=l,
            index=data_df.columns)
        .rename_axis('variable')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_fast_file_status(site: str, run_time: dt.datetime=None) -> dict:
    """
    Get the newest fast file.

    Args:
        site: name of site.
        run_time (optional): the time to use for calculation of time since
            valid variables reported. If not supplied, system time at runtime is
            used. Defaults to None.

    Returns:
        Dictionary containing file name and age in days.

    """

    # Create dummy dictionary to pass if error
    dummy_dict = {'file_name': 'No files', 'days_since_last_record': None}

    # Use passed run time if exists, otherwise get current
    if run_time is None:
        run_time = dt.datetime.now()

    # Get data path to raw file
    try:
        data_path = paths_mngr.get_local_stream_path(
            resource='data',
            stream='flux_fast',
            site=site,
            subdirs=['TOB3'],
            check_exists=True
            )
    except FileNotFoundError:
        return dummy_dict

    # Get file and age in days
    try:
        file = max(data_path.rglob('TOB3*.dat'), key=os.path.getctime).name
        days = (
            (
                run_time - dt.datetime.strptime(
                    '-'.join(file.replace('.dat', '').split('_')[-3:]),
                    '%Y-%m-%d'
                    )
                )
            .days - 1
            )
        return {'file_name': file, 'days_since_last_record': days}
    except ValueError:
        return dummy_dict
#------------------------------------------------------------------------------

###############################################################################
### END DATA COLLATION FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN EXCEL DATA WRITE FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def _write_file_status(
        files_df: pd.DataFrame, writer: pd.ExcelWriter, sheet_name: str,
        run_time: dt.datetime
        ) -> None:
    """
    Write the file status of all site files to a spreadsheet.

    Args:
        files_df: data to write.
        writer: pandas excel writer object to write to.
        run_time (optional): the time to use for calculation of time since
            valid variables reported. If not supplied, system time at runtime is
            used. Defaults to None.

    Returns:
        None.

    """

    # Prepend the run date and time to the summary spreadsheet
    _write_time_frame(
        xl_writer=writer,
        sheet=sheet_name,
        run_time=run_time
        )

    # Output and format the results
    (
        files_df.style.apply(_get_style_df, axis=None)
        .to_excel(
            writer, sheet_name=sheet_name, index=False, startrow=1
            )
        )
    _set_column_widths(
        df=files_df, xl_writer=writer, sheet=sheet_name
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_site_data_status(
        site: str, data_df: pd.DataFrame, writer: pd.ExcelWriter,
        run_time: dt.datetime
        ) -> None:
    """
    Write the data status of an individual site to a spreadsheet.

    Args:
        site: name of site.
        data_df: data to write.
        writer: pandas excel writer object to write to.
        run_time (optional): the time to use for calculation of time since
            valid variables reported. If not supplied, system time at runtime is
            used. Defaults to None.

    Returns:
        None.

    """

    # Prepend the run date and time to the spreadsheet
    _write_time_frame(
        xl_writer=writer,
        sheet=site,
        run_time=_get_site_time(site=site, run_time=run_time)
        )

    # Apply the formatting
    (
        data_df.style.apply(
            _get_style_df,
            column_name='days_since_last_valid_record',
            axis=None
            )
        .to_excel(
            writer, sheet_name=site,  index=False, startrow=1,
            )
        )

    _set_column_widths(
        df=data_df, xl_writer=writer, sheet=site
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_key(writer) -> None:
    """Write key that maps the interval (in days) between run time and last data."""

    sheet_name = 'Key'
    colours = ['green', 'yellow', 'orange', 'red']
    intervals = ['< 1 day', '1 <= day(s) < 3', '3 <= days < 7', 'days >= 7']
    key_df = pd.DataFrame([colours, intervals], index=['colour', 'interval']).T
    (
        key_df.style.apply(_get_key_formatter, axis=None)
        .to_excel(writer, sheet_name=sheet_name, index=False)
        )
    _set_column_widths(
        df=key_df, xl_writer=writer, sheet=sheet_name
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_site_time(site, run_time=None):
    """
    Correct server time to site-based local standard time.

    Args:
        site: name of site.
        run_time (optional): the time to use for calculation of time since
            valid variables reported. If not supplied, system time at runtime is
            used. Defaults to None.

    Returns:
        TYPE: DESCRIPTION.

    """

    server_utc_offset = time.localtime().tm_gmtoff / 3600
    if run_time is None:
        run_time = dt.datetime.now()
    return (
        run_time -
        dt.timedelta(
            hours=
            server_utc_offset -
            sd_mngr.get_single_site_details(site, 'UTC_offset')
            )
        )
#------------------------------------------------------------------------------

###############################################################################
### END EXCEL DATA WRITE FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN EXCEL FORMATTER FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def _get_style_df(
        df: pd.DataFrame, column_name: str='days_since_last_record'
        ) -> pd.DataFrame:
    """
    Generate a style df of same dimensions, index and columns, with colour
    specifications in the appropriate column.

    Args:
        df: the dataframe.
        column_name (optional): the column to parse for setting of colour.
            Defaults to 'days_since_last_record'.

    Returns:
        style_df: Formatter dataframe containing empty strings for all
            non-coloured cells and colour formatting for coloured cells.

    """

    style_df = pd.DataFrame('', index=df.index, columns=df.columns)
    style_df[column_name] = df[column_name].apply(_get_colour, xl_format=True)
    return style_df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_key_formatter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a dataframe that can be used to format the key spreadsheet.

    Args:
        df (pd.DataFrame): The dataframe for which to return the formatter.

    Returns:
        the formatted dataframe.

    """

    this_df = df.copy()
    this_df.loc[:, 'colour']=[0,2,5,7]
    return _get_style_df(df=this_df, column_name='colour')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _set_column_widths(
        df: pd.DataFrame, xl_writer: pd.ExcelWriter, sheet: str,
        add_space: int=2
        ) -> None:
    """
    Set the xl column widths for whatever is largest (header or content).

    Args:
        df: the frame for which to set the widths.
        xl_writer: pandas excel writer object to which to write.
        sheet: sheet name to write columns widths to.
        add_space (optional): arbitrary space buffer. Defaults to 2.

    Returns:
        None.

    """

    for i, column in enumerate(df.columns):
        col_width = max(
            df[column].astype(str).map(len).max(),
            len(column)
            )
        xl_writer.sheets[sheet].set_column(i, i, col_width + add_space)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_colour(n: int, xl_format: bool=False) -> str:
    """
    Get the formatted colour based on value of n.

    Args:
        n: the number for evaluation.
        xl_format (optional): formats string to excel default if True.
            Defaults to False.

    Returns:
        formatted colour string for pandas styler.

    """

    try:
        n = int(n)
        if n < 1:
            colour = 'green'
        if 1 <= n < 3:
            colour = 'blue'
        if 3 <= n < 5:
            colour = 'magenta'
        if 5 <= n < 7:
            colour = 'orange'
        if n >= 7:
            colour = 'red'
    except ValueError:
        if isinstance(n, str):
            colour = 'red'
        elif np.isnan(n):
            colour = None
    if colour == None:
        return ''
    if xl_format:
        return f'background-color: {colour}'
    return colour
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_time_frame(
        xl_writer: pd.ExcelWriter, sheet: str, run_time: dt.datetime,
        zone=None
        ) -> None:
    """
    Write the run date and time to the first line of a spreadsheet.

    Args:
        xl_writer: pandas excel writer object to which to write.
        sheet: sheet name to write columns widths to.
        run_time: time to write to spreadsheet.
        zone (optional): string representing timezone. Defaults to None.

    Returns:
        None.

    """

    if zone is None:
        zone = ''
    frame = (
        pd.DataFrame(
            [f'RUN date/time: {run_time.strftime("%Y-%m-%d %H:%M")} {zone}'],
            index=[0]
            )
        .T
        )
    frame.to_excel(
        xl_writer, sheet_name=sheet, index=False, header=False
        )
#------------------------------------------------------------------------------

###############################################################################
### END EXCEL FORMATTER FUNCTIONS ###
###############################################################################

if __name__=='__main__':

    write_status_xlsx()