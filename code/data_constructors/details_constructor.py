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
def get_site_info(site: str) -> None:
    """
    Collate and write site information required for RTMC files.

    Args:
        site: name of site.

    Returns:
        None.

    """

    # Get site info
    logger.info(f'  Getting information for {site}...')
    site_info = (
        paths.get_internal_configs(config_name='site_metadata')[site]
        )
    site_info['start_year'] = str(
        dt.datetime.strptime(site_info['date_commissioned'], '%Y-%m-%d').year
        )

    # Get sunrise / sunset info
    logger.info('    Getting sunrise / sunset')
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
            }
        )

    # # Get fast file info
    # logger.info('Getting latest 10Hz file...')
    # try:
    #     file = fdf.get_last_formatted_fast_file(
    #         site=site,
    #         abs_path=False
    #         )
    #     fast_file_info = {'10Hz_file': file}
    #     logger.info(f'Found fast file {file}')
    # except FileNotFoundError:
    #     fast_file_info = {'10Hz_file': 'No files'}
    #     logger.error('No files found')

    # Get flux logger info
    logger.info('    Getting flux logger information...')
    md_mngr = metadata.MetaDataManager(site=site)
    file = md_mngr.data_path / md_mngr.flux_file
    logger_info = md_mngr.get_file_attributes(file=md_mngr.flux_file)[LOGGER_SUBSET].to_dict()

    # Get missing data info
    logger.info('    Getting missing data percentage...')
    missing_info = (
        fh.DataHandler(file=md_mngr.data_path / file).get_missing_records()
        )
    missing_info.pop('n_missing')

    # Collate
    logger.info('    Done!')
    return site_info | sun_info | logger_info | missing_info
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def site_info_2_json(site_list):

    logger.info('Generating site information json...')
    data_list = []
    for site in site_list:
        site_info = get_site_info(site=site)
        for key, value in site_info.items():
            if value is None:
                site_info[key] = ''
        data_list.append({'site': site} | site_info)
    output_path = paths.get_local_stream_path(
        resource='network',
        stream='status',
        file_name='site_info.json'
        )
    with open(file=output_path, mode='w', encoding='utf-8') as f:
        json.dump(data_list,f,indent=4)
    logger.info('Done!')
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
