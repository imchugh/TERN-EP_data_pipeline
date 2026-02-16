# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 14:42:54 2024

@author: jcutern-imchugh
"""

###############################################################################
### BEGIN INITS ###
###############################################################################

import datetime as dt
import geojson
import logging
import numpy as np
import pandas as pd
import pathlib
import time

#------------------------------------------------------------------------------

from csi_loggers import logger_functions as lf
from data_constructors.convert_calc_filter import filter_range
from file_handling.nc_io import NCReader
from file_handling.file_handler import DataHandler
from file_handling import fast_data_filer as fdf
from managers import paths
from managers import site_details as sd
from managers.metadata import PFPNameParser, MetaDataManager

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

sd_mngr = sd.SiteDetailsManager(use_local=True)
name_parser = PFPNameParser()
SUBSET = ['Fco2', 'Fh', 'Fe', 'Fsd']
logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN MAIN FUNCTIONS ###
###############################################################################

# -----------------------------------------------------------------------------
def network_status_to_geojson(site_list: list) -> dict:
    """
    Generate a GeoJSON file describing network status for all sites.

    Args:
        site_list: list of sites to process.

    Returns:
        dict mapping site -> status result
    """

    logger.info(
        "status_geojson_generation_start",
        extra={"site_count": len(site_list)},
    )

    features: list[geojson.Feature] = []
    results: dict = {}

    for site in site_list:
        
        try:
            feature = build_site_feature(site)
            features.append(feature)
            results[site] = {"status": "success"}

        except (KeyError, FileNotFoundError, IndexError) as e:
            results[site] = {"status": "failure", "error": str(e)}
            logger.warning(
                "site_status_skipped",
                extra={"site": site, "error": str(e)},
            )

        except Exception as e:
            results[site] = {"status": "failure", "error": str(e)}
            logger.error(
                "site_status_failed",
                extra={"site": site, "error": str(e)},
                exc_info=True,
            )

    geojson_obj = build_geojson_feature_collection(features)

    output_path = paths.get_local_stream_path(
        resource="network",
        stream="status",
        file_name="network_status_test.json",
    )

    write_geojson(geojson_obj, output_path)

    logger.info("status_geojson_generation_complete")

    return results
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def write_geojson(obj: dict, output_path) -> None:
    """
    Write GeoJSON object to disk.
    """
    with open(output_path, mode="w", encoding="utf-8") as f:
        geojson.dump(obj, f, indent=4)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def build_geojson_feature_collection(
        features: list[geojson.Feature], 
        ) -> geojson.FeatureCollection:

    collection = geojson.FeatureCollection(features)

    collection["metadata"] = {
        "rundatetime": dt.datetime.now(dt.timezone.utc).isoformat()
    }

    return collection
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def build_site_feature(site: str) -> geojson.Feature:
    """
    Build a GeoJSON feature for a single site.
    """

    status_dict = get_site_data_status(site, subset=SUBSET)

    # Compute overall min days since last record
    days_since_last_record = min(
        v["days_since_last_record"]
        for v in status_dict.values()
        if isinstance(v["days_since_last_record"], int)
    )

    properties = {
        "days_since_last_record": days_since_last_record,
        **{
            var: v["days_since_last_valid_record"]
            for var, v in status_dict.items()
        },
    }

    latitude = sd_mngr.df.loc[site, "latitude"]
    longitude = sd_mngr.df.loc[site, "longitude"]

    return geojson.Feature(
        id=site,
        geometry=geojson.Point(coordinates=[latitude, longitude]),
        properties=properties,
    )
# -----------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_data_status(site, file_index=-1, subset=None, standardise=True):
    
    # Inits
    input_path = paths.get_local_stream_path(
        resource='homogenised_data',
        stream='nc',
        subdirs=[site]
        )
    
    # Get file
    file_list = sorted(list(input_path.glob('*.nc')))
    if len(file_list) == 0:
        raise FileNotFoundError(
            f'No .nc files found for site {site} in directory {input_path}!'
            )

    # Pass to get_data_status
    file = file_list[file_index]
    site_time = _get_site_time(site=site, run_time=dt.datetime.now())
    return get_data_status(file=file, site_time=site_time, subset=subset)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data_status(file, site_time, subset=None, standardise=True):
    """
    Read netcdf file to get data status for .nc status file.

    Args:
        site_time: time to use for comparison of data age.

    Returns:
        dataframe containing results.

    """

    # Get the data
    df = NCReader(nc_file=file).get_dataframe()
    
    rename_map = {}
    subset = subset or list(df.columns)
    if standardise:
        for std_name in subset:
            raw_name = resolve_standard_name(df.columns, std_name)
            rename_map[raw_name] = std_name
    else:
        rename_map = {col: col for col in df.columns if col in subset}
       
    # Subset + rename DataFrame
    working_df = df[list(rename_map.keys())].rename(columns=rename_map)
    
    rslt = {}
    for variable in working_df.columns:

        # Get the maxima and minima (some non-standard variables are allowed
        # when not recognised by the parser, so handle TypeErrors by setting
        # the valid range to nans, which will leave the range unchanged)
        try:
            info = name_parser.parse_variable_name(variable_name=variable)
            valid_range = (info['plausible_min'], info['plausible_max'])
        except TypeError:
            valid_range = (-9999, 9999)
            
        # Parse the variable
        rslt[variable] = (
            parse_variable(
                variable=working_df[variable],
                var_range=valid_range,
                site_time=site_time
                )
            )

    return rslt # pd.DataFrame(rslt_list, index=SUBSET)
#------------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def resolve_standard_name(columns: list[str], std_name: str) -> str:
    """
    Resolve a standard variable name to a raw column in a file.
    
    Args:
        columns: list of DataFrame column names
        std_name: the standard name to find
    
    Returns:
        raw column name matching the standard name
    
    Raises:
        KeyError if no match is found
    """
    # Step 1: fast prefix filter
    candidates = [col for col in columns if col == std_name or col.startswith(std_name + "_")]

    if not candidates:
        raise KeyError(f"No column found matching standard name '{std_name}'")

    # Step 2: optional verification via name_parser
    if len(candidates) > 1:
        verified = []
        for col in candidates:
            try:
                info = name_parser.parse_variable_name(col)
                if info["standard_name"] == std_name:
                    verified.append(col)
            except Exception:
                continue
        if verified:
            candidates = verified

    # Step 3: deterministic replicate selection
    return sorted(candidates)[0]
# -----------------------------------------------------------------------------

#------------------------------------------------------------------------------
def parse_variable(
        variable: pd.Series, var_range: tuple, site_time: dt.datetime
        ) -> dict:
    """
    Parse the variable for statistics.

    Args:
        variable: variable data to parse.
        var_range: lower and upper range limits.

    Returns:
        dicitionary containing status results.

    """

    # Create record structure
    rec = {
        'days_since_last_record': 'N/A',
        'last_valid_value': None,
        'last_valid_record_datetime': None,
        'last_24hr_pct_valid': 0,
        'days_since_last_valid_record': 'N/A'
        }


    # Get the last record (valid or not); return default record structure if not   
    if variable.empty:
        logger.debug(f"No records available for variable {variable.name}")
        return rec

    
    # Get time since the last record in days and add to the 
    dt_last_rec = (site_time - variable.index[-1]).days
    rec['days_since_last_record'] = dt_last_rec
    
    # Filter the data
    filtered_variable = (
        filter_range(
            series=variable.copy(),
            min_val=var_range[0], max_val=var_range[1]
            )
        .dropna()
        )

    # If no good data, return the record structure
    if filtered_variable.empty:
        logger.debug(
            'No valid records remain after filtering for variable '
            f'{variable.name}!'
            )
        return rec

    # Do the stats
    lvr = filtered_variable.iloc[-1]
    last_valid_time = filtered_variable.index[-1]
    days_since_valid = (site_time - last_valid_time).days
    pct_valid = 0
    
    # Calculate percent good data in last 24 hours
    if days_since_valid == 0:
        window_start = site_time - dt.timedelta(days=1)
        raw_window = variable.loc[window_start:site_time]
        if not raw_window.empty:
            valid_window = filtered_variable.loc[window_start:site_time]
            pct_valid = round(
                len(valid_window) / len(raw_window) * 100
                )
                                                 
    # Fill the record
    rec.update(
        {
            "last_valid_value": lvr,
            "last_valid_record_datetime": last_valid_time,
            "last_24hr_pct_valid": pct_valid,
            "days_since_last_valid_record": days_since_valid,
        }
    )
    
    return rec
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def write_status_geojson(site_list: list) -> None:
#     """
#     Write status information to a geojson file.

#     Args:
#         site_list: list of sites to process.

#     Returns:
#         None.

#     """

#     # Inits
#     input_path = paths.get_local_stream_path(
#         resource='homogenised_data',
#         stream='nc',
#         )
#     output_path = paths.get_local_stream_path(
#         resource='network',
#         stream='status',
#         file_name='network_status.json'
#         )
#     rslt_list = []

#     logger.info('Scanning network: ')

#     # Iterate over sites
#     for site in site_list:

#         logger.info(f'    - retrieving status for site {site}...')

#         # Get the file name
#         try:
#             file = _get_file(target_dir=input_path, site=site)
#         except FileNotFoundError:
#             logger.error(msg='There was a problem: ', exc_info=True)
#             continue

#         logger.info(f'      - Parsing file {file.name}...')

#         # Parse the variables
#         df = parse_slow_data_status_4_geojson(file=file, site_time=_get_site_time(site))
#         rslt = {'days_since_last_record': df.days_since_last_record.max()}
#         rslt.update(df.days_since_last_valid_record.to_dict())
#         rslt_list.append(
#             geojson.Feature(
#                 id=site,
#                 geometry=geojson.Point(coordinates=[
#                     sd_mngr.df.loc[site, 'latitude'],
#                     sd_mngr.df.loc[site, 'longitude']
#                     ]),
#                 properties=rslt
#                 )
#             )
        
#         logger.info('      - Successfully parsed...')

#         logger.info('    ... done!')

#     logger.info('Scan complete - dumping to file...')

#     json_obj = geojson.FeatureCollection(rslt_list)
#     json_obj['metadata'] = {
#         'rundatetime': dt.datetime.strftime(
#             dt.datetime.now(), '%Y-%m-%d %H:%M:%S'
#             )
#         }
#     with open(file=output_path, mode='w', encoding='utf-8') as f:
#         geojson.dump(json_obj,f,indent=4)

#     logger.info('... done!')
# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_status_xlsx(site_list) -> None:
    """
    Write status information to an xlsx file.

    Args:
        site_list: list of sites to process.

    Returns:
        None.

    """

    # Inits
    nc_input_path = paths.get_local_stream_path(
        resource='homogenised_data',
        stream='nc',
        )
    output_path = paths.get_local_stream_path(
        resource='network',
        stream='status',
        file_name='network_status.xlsx'
        )
    run_time = dt.datetime.now()

    slow_file_status_list = []
    fast_file_status_list = []
    site_data_status_dict = {}

    logger.info('Evaluating site status: ')

    # Collate data using single call to metadata manager per site
    for site in site_list:

        logger.info(f'    - {site}')

        # Parse the slow data from the netcdf file
        try:
            file = _get_file(target_dir=nc_input_path, site=site)
        except FileNotFoundError:
            logger.error(msg='There was a problem: ', exc_info=True)
            continue

        # Get the site time
        site_time = _get_site_time(site, run_time=run_time)

        # Get the status of the variables inside the netcdf file
        slow_data_status = parse_slow_data_status_4_xlsx(
            file=file,
            site_time=site_time
            )

        # Get the status of the files that contributed the data to the netcdf
        # file
        try:
            slow_file_status = parse_file_status_4_xlsx(
                site=site,
                site_time=site_time
                )
        except FileNotFoundError:
            logger.error(msg='There was a problem: ', exc_info=True)
            continue

        # Append them to the list
        slow_file_status_list.append(slow_file_status)
        site_data_status_dict[site] = slow_data_status
        fast_file_status_list.append(
            {'site': site} |
            get_fast_file_status(site=site)
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

            try:

                _write_site_data_status(
                    site=site,
                    data_df=site_data_status_dict[site],
                    writer=writer,
                    site_time=_get_site_time(site, run_time=run_time)
                    )

            except KeyError:

                continue

        # Write key
        _write_key(writer=writer)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_file(target_dir: pathlib.Path, site: str) -> pathlib.Path:
    """
    Find the newest .nc file to import.

    Args:
        target_dir: base directory to search.
        site: name of site.

    Raises:
        FileNotFoundError: raised if either directory does not exist or no
        files found.

    Returns:
        absolute path to file.

    """

    # Get the file name
    site_dir = target_dir / site
    if not site_dir.exists():
        raise FileNotFoundError(
            f'Directory {str(site_dir)} does not exist!'
            )
    file_list = sorted(list(site_dir.glob('*.nc')))
    if len(file_list) == 0:
        raise FileNotFoundError(
            f'No .nc files found for site {site} in directory {site_dir}!'
            )
    return file_list[-1]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_sites_online():

    logger.info('Running logger clock checks')

    vpn_configs = paths.get_internal_configs('vpn_ip')
    input_d = {
        key: value['ip'].split('.')[-1] for key, value in vpn_configs.items()
        }
    rslt = {}
    for site, ip_addr in input_d.items():
        print(site)
        time = dt.datetime.now()
        try:
            site_rslt = (
                {'response': 'True'} |
                check_logger_online(ip_addr=ip_addr)
                )
            site_rslt['time'] = (
                dt.datetime.strptime(site_rslt['time'], '%Y-%m-%dT%H:%M:%S.%f')
                )
            site_rslt['offset'] = (time - site_rslt['time']).seconds
            expected_offset = None
            rslt[site] = site_rslt
        except ConnectionError:
            rslt[site] = {'response': False}
    return pd.DataFrame(rslt).T
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_logger_online(ip_addr):

    return (
        {'time': lf.clock_check(ip_addr=ip_addr).pop('time')} |
        lf.get_used_space(ip_addr=ip_addr, source='CRD')
        )
#------------------------------------------------------------------------------

###############################################################################
### END MAIN FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN DATA COLLATION FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def parse_slow_data_status_4_geojson(file, site_time):
    """
    Read netcdf file to get data status for .nc status file.

    Args:
        site_time: time to use for comparison of data age.

    Returns:
        dataframe containing results.

    """

    # Get the data
    df = NCReader(nc_file=file).get_dataframe()

    # Iterate over the substrings to collect the import flux quantities
    rslt_list = []
    for substr in SUBSET:

        # Handle multiple replicates
        variable = sorted(
            var for var in df.columns if var.startswith(substr)
            )[0]

        # Get the maxima and minima (some non-standard variableas are allowed
        # when not recognised by the parser, so handle TypeErrors by setting
        # the valid range to nans, which will leave the range unchanged)
        try:
            info = name_parser.parse_variable_name(variable_name=variable)
            valid_range = (info['plausible_min'], info['plausible_max'])
        except TypeError:
            valid_range = (-9999, 9999)
        # Parse the variable
        rslt_list.append(
            _parse_variable(
                variable=df[variable],
                var_range=valid_range,
                site_time=site_time
                )
            )

    return pd.DataFrame(rslt_list, index=SUBSET)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def parse_slow_data_status_4_xlsx(
        file: pathlib.Path, site_time: dt.datetime
        ) -> pd.DataFrame:
    """
    Read netcdf file to get data status for xlsx status file.

    Args:
        site_time: time to use for comparison of data age.

    Returns:
        dataframe containing results.

    """

    # Get the data
    df = NCReader(nc_file=file).get_dataframe()

    # Iterate over the substrings to collect the import flux quantities
    rslt_list = []

    # Iterate over all variables
    for variable in df.columns:

        # Get the maxima and minima (some non-standard variableas are allowed
        # when not recognised by the parser, so handle TypeErrors by setting
        # the valid range to nans, which will leave the range unchanged)
        try:
            info = name_parser.parse_variable_name(variable_name=variable)
            valid_range = (info['plausible_min'], info['plausible_max'])
        except TypeError:
            valid_range = (-9999, 9999)

        # Parse the variable
        rslt_list.append(
            _parse_variable(
                variable=df[variable],
                var_range=valid_range,
                site_time=site_time
                )
            )

    # Format and return the results
    output_df = pd.DataFrame(rslt_list, index=df.columns)
    output_df.index.name = 'variable'
    return output_df.reset_index()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def parse_file_status_4_xlsx(site: str, site_time: dt.datetime) -> pd.DataFrame:
    """
    Read raw files to get raw file status for xlsx status file.

    Args:
        site: name of site.
        site_time: time to use for comparison of data age.

    Returns:
        dataframe containing results.

    """

    # Get the metadata manager
    mdm = MetaDataManager(site=site)

    # Grab the site-adjusted run time
    site_time = _get_site_time(site=site, run_time=site_time)

    # Get the logger / file particulars (currently must handle sites with no
    # logger or table)
    try:
        files_df = (
            mdm.site_variables[['logger', 'table', 'file']]
            .drop_duplicates()
            .reset_index(drop=True)
            .set_index(keys='file')
            )
    except KeyError:
        files_df = (
            mdm.site_variables[['file']]
            .drop_duplicates()
            .reset_index(drop=True)
            .set_index(keys='file')
            .assign(logger=None, table=None)
            )

    # Get the file attributes
    attrs_df = (
        pd.concat(
            [mdm.get_file_attributes(x) for x in mdm.list_files()],
            axis=1
            )
        .T
        .drop(['station_name', 'table_name'], axis=1)
        )
    attrs_df.index = mdm.list_files()

    # Get the percentage missing data for each file
    # Note that here, we DONT try to concatenate if the file is an EP master
    # file because the concatenation is already handled
    missing_list = []
    do_concat = False
    for file in mdm.list_files(abs_path=True):
        do_concat = True
        if attrs_df.loc[file.name, 'format'] != 'TOA5':
            do_concat = False
        missing_list.append(
            DataHandler(file=file, concat_files=do_concat)
            .get_missing_records()
            )
    missing_df = pd.DataFrame(
        data=missing_list, index=mdm.list_files()
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
        .assign(site=site)
        .reset_index()
        )

    return output_df.set_index(keys=['site', 'logger', 'table'])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_variable(
        variable: pd.Series, var_range: tuple, site_time: dt.datetime
        ) -> dict:
    """
    Parse the variable for statistics.

    Args:
        variable: variable data to parse.
        var_range: lower and upper range limits.

    Returns:
        dicitionary containing status results.

    """

    # Before anything, get the last record (valid or not)
    try:
        dt_last_rec = (site_time - variable.index[-1]).days
    except IndexError:
        dt_last_rec = 'N/A'

    # Filter the data
    filtered_variable = (
        filter_range(
            series=variable.copy(),
            min_val=var_range[0], max_val=var_range[1]
            )
        .dropna()
        )

    # If no good data, return the dummy
    if len(filtered_variable) == 0:

        return {
            'days_since_last_record': dt_last_rec,
            'last_valid_value': None,
            'last_valid_record_datetime': None,
            'last_24hr_pct_valid': 0,
            'days_since_last_valid_record': 'N/A'
            }

    # If good data do the stats
    lvr = filtered_variable.iloc[-1]
    dt_lvr = filtered_variable.index[-1]
    how_old = (site_time - dt_lvr).days
    pct_valid = 0
    if not how_old > 0:
        pct_valid = round(
            len(filtered_variable.loc[site_time - dt.timedelta(days=1): site_time]) /
            len(variable.loc[site_time - dt.timedelta(days=1): site_time]) *
            100
            )

    # Populate the dict and return
    return {
        'days_since_last_record': dt_last_rec,
        'last_valid_value': lvr,
        'last_valid_record_datetime': dt_lvr,
        'last_24hr_pct_valid': pct_valid,
        'days_since_last_valid_record': how_old
        }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_fast_file_status(site: str) -> dict:
    """
    Get the newest fast file.

    Args:
        site: name of site.

    Returns:
        Dictionary containing file name and age in days.

    """

    # INITS
    dummy_dict = {'file_name': 'No files', 'days_since_last_record': None}
    run_time = dt.datetime.now()

    # Get file
    try:
        rslt = fdf.get_last_formatted_fast_file(site=site, abs_path=False)
    except FileNotFoundError:
        return dummy_dict

    # Get file and age in days
    days = (
        (
            run_time - dt.datetime.strptime(
                '-'.join(rslt.replace('.dat', '').split('_')[-3:]),
                '%Y-%m-%d'
                )
            )
        .days - 1
        )
    return {'file_name': rslt, 'days_since_last_record': days}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_site_time(site: str, run_time: dt.datetime=None) -> dt.datetime:
    """
    Correct server time to site-based local standard time.

    Args:
        site: name of site.
        run_time (optional): the time to use for calculation of time since
            valid variables reported. If not supplied, system time at runtime is
            used. Defaults to None.

    Returns:
        site local standard time equivalent of AEST server time.

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
        site_time: dt.datetime
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
        run_time=site_time
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
