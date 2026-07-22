# -*- coding: utf-8 -*-
"""
Created on Mon Aug 26 11:46:04 2024

@author: jcutern-imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import datetime as dt
import logging
import pandas as pd
import pathlib
import json

#------------------------------------------------------------------------------

# import utils.metadata_handlers as mh
from file_handling import file_handler as fh, fast_data_filer as fdf
from data_constructors.convert_calc_filter import TimeFunctions
from file_handling import file_io as io
from managers import metadata, paths

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

logger = logging.getLogger(__name__)
DETAILS_SUBSET = []
LOGGER_SUBSET = [
    'format', 'station_name', 'logger_type', 'serial_num', 'OS_version',
    'program_name'
    ]
SITE_METADATA = paths.get_internal_configs(config_name='site_metadata')

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def write_site_info(site: str) -> None:
    """
    Collate and write site information required for RTMC files.

    Args:
        site: name of site.

    Returns:
        None.

    """

    # Get site info
    logger.info('Getting site information...')
    site_info = (
        paths.get_internal_configs(config_name='site_metadata')[site]
        # paths.get_local_config_file(config_stream='all_site_metadata')[site]
        )
    site_info['start_year'] = str(
        dt.datetime.strptime(site_info['date_commissioned'], '%Y-%m-%d').year
        )

    # Get sunrise / sunset info
    logger.info('Getting sunrise / sunset')
    midnight_datetime = dt.datetime.combine(
        date=dt.datetime.now().date(), time=dt.time()
        )
    time_getter = TimeFunctions(
        lat=site_info['latitude'],
        lon=site_info['longitude'],
        elev=site_info['elevation'],
        date=midnight_datetime
        )
    sun_info = {
        'sunrise': time_getter.get_next_sunrise().strftime('%H:%M'),
        'sunset': time_getter.get_next_sunset().strftime('%H:%M'),
        }
    site_info.update(
        {
            'time_zone': time_getter.time_zone,
            'UTC_offset': time_getter.utc_offset.seconds / 3600
            }
        )

    # Get fast file info
    logger.info('Getting latest 10Hz file...')
    try:
        file = fdf.get_last_formatted_fast_file(
            site=site,
            abs_path=False
            )
        fast_file_info = {'10Hz_file': file}
        logger.info(f'Found fast file {file}')
    except FileNotFoundError:
        fast_file_info = {'10Hz_file': 'No files'}
        logger.error('No files found')

    # Get flux logger info
    logger.info('Getting flux logger information...')
    nc_file_list = paths.get_internal_configs('tasks').Site.tolist()
    if site in nc_file_list:
        from managers import metadata
        md_mngr = metadata.MetaDataManager(site=site)
        file = md_mngr.data_path / md_mngr.flux_file
        logger_info = md_mngr.get_file_attributes(file=md_mngr.flux_file)[LOGGER_SUBSET].to_dict()
    else:
        from utils import metadata_handlers as mh
        md_mngr = mh.MetaDataManager(site=site, variable_map='vis')
        file = md_mngr.get_variable_attributes(variable='Fco2', return_field='file')
        logger_info = md_mngr.get_file_attributes(file=file)[LOGGER_SUBSET].to_dict()

    # Get missing data info
    logger.info('Getting missing data percentage...')
    missing_info = (
        fh.DataHandler(file=md_mngr.data_path / file).get_missing_records()
        )
    missing_info.pop('n_missing')

    # Collate
    logger.info('Collating sources...')
    data = (
        pd.DataFrame(
            data=(
                site_info |
                sun_info |
                fast_file_info |
                logger_info |
                missing_info
                ),
            index=[midnight_datetime]
            )
        .rename_axis('TIMESTAMP')
        .reset_index()
        )

    # Make the headers
    headers = pd.DataFrame(
        data={'Units': 'unitless', 'Samples': 'Smp'},
        index=pd.Index(data.columns, name='variables')
        )
    headers.loc['TIMESTAMP']=['TS', '']

    # Make the info
    info = dict(zip(io.INFO_FIELD_NAMES, io.FILE_CONFIGS['TOA5']['dummy_info']))
    info.update({'station_name': site, 'table_name': 'site_details'})

    # Set the output path
    output_path = (
        paths.get_local_stream_path(
            resource='homogenised_data', stream='TOA5', site=site
            ) /
        f'{site}_details.dat'
        )

    # Write to file
    logger.info('Writing to file')
    io.write_data_to_file(
        headers=headers, data=data, abs_file_path=output_path, info=info)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_info(site: str, midnight: dt.datetime | None=None) -> dict:
    """
    Collate and write site information required for RTMC files.

    Args:
        site: name of site.

    Returns:
        None.

    """

    # Use today's midnight if not provided
    if midnight is None:
        midnight = dt.datetime.combine(dt.datetime.now().date(), dt.time())

    # Site metadata
    try:
        site_info = SITE_METADATA[site].copy()
    except KeyError:
        raise KeyError(f"No metadata found for site '{site}'")

  # Add derived field
    site_info['start_year'] = str(
        dt.datetime.strptime(
            site_info.get('date_commissioned', '1900-01-01'), '%Y-%m-%d').year
        )  

    # Get sunrise / sunset info
    try:
        time_getter = TimeFunctions(
            lat=site_info['latitude'],
            lon=site_info['longitude'],
            elev=site_info['elevation'],
            date=midnight
            )
        sun_info = {
            'sunrise': time_getter.get_next_sunrise().strftime('%H:%M'),
            'sunset': time_getter.get_next_sunset().strftime('%H:%M'),
            'time_zone': getattr(time_getter, 'time_zone', ''),
            'UTC_offset': getattr(
                time_getter, 'utc_offset', dt.timedelta()
                ).total_seconds() / 3600
            }
    except Exception as e:
        logger.debug(
            f"Sunrise/sunset computation failed for site '{site}': {e}"
            )
        sun_info = {
            'sunrise': '', 'sunset': '', 'time_zone': '', 'UTC_offset': ''
            }
        
    # Check for fast data file
    try:
        fast_file = fdf.get_last_formatted_fast_file(
            site=site, abs_path=False
            )
    except FileNotFoundError:
        logger.debug(f"No 10Hz fast file found for site '{site}'")
        fast_file = 'No files'
    except Exception as e:
        logger.debug(f"Error retrieving 10Hz fast file for site '{site}': {e}")
        fast_file = 'No files'
    fast_file_info = {'10Hz_file': fast_file}
  
    # Get flux logger info
    try:
        md_mngr = metadata.MetaDataManager(site=site)
        flux_file_path = md_mngr.data_path / md_mngr.flux_file
        logger_info = md_mngr.get_file_attributes(
            file=md_mngr.flux_file
            )[LOGGER_SUBSET].to_dict()
    except Exception as e:
        logger.debug(
            f"Flux logger info retrieval failed for site '{site}': {e}"
            )
        logger_info = {k: '' for k in LOGGER_SUBSET}

    # Get missing data info
    missing_info = {}
    if flux_file_path:
        try:
            missing_info = fh.DataHandler(file=flux_file_path).get_missing_records()
            missing_info.pop('n_missing', None)
        except Exception as e:
            logger.debug(
                f"Missing data computation failed for site '{site}': {e}"
                )

    # Combine all
    combined_info = (
        site_info | sun_info | fast_file_info | logger_info | missing_info
        )
    combined_info = {
        k: ('' if v is None else v) for k, v in combined_info.items()
        }
    
    return combined_info
#------------------------------------------------------------------------------



def site_info_2_json(site_list: list) -> dict:
    """
    Build site metadata JSON and return per-site status results.
    """

    logger.info(
        'site_info_generation_start', extra={'site_count': len(site_list)})

    data_list = []
    results = {}

    for site in site_list:
        
        try:
            
            site_info = get_site_info(site=site)
            data_list.append({"site": site} | site_info)
            results[site] = {"status": "success"}

        except KeyError as e:
            results[site] = {
                "status": "failure",
                "error": str(e),
            }
            logger.warning(
                'site_info_skipped',
                extra={"site": site, "error": str(e)},
            )

        except Exception as e:
            results[site] = {
                "status": "failure",
                "error": str(e),
            }
            logger.error(
                "site_info_failed",
                extra={"site": site},
                exc_info=True,
            )

    output_path = paths.get_local_stream_path(
        resource="network",
        stream="status",
        file_name="site_info.json",
    )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_list, f, indent=4)

    logger.info(
        "site_info_generation_end",
        extra={
            "sites_total": len(site_list),
            "sites_success": sum(r["status"] == "success" for r in results.values()),
            "sites_failure": sum(r["status"] == "failure" for r in results.values()),
        },
    )

    return results
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
