# -*- coding: utf-8 -*-
"""
Created on Mon Aug 26 11:46:04 2024

@author: jcutern-imchugh
"""

import datetime as dt
import logging
import pandas as pd

#------------------------------------------------------------------------------
import utils.configs_getters as cg
import utils.metadata_handlers as mh
import file_handling.file_handler as fh
from data_constructors.convert_calc_filter import TimeFunctions
from file_handling import file_io as io
from utils.paths_manager import PathsManager

#------------------------------------------------------------------------------
# INITS #
logger = logging.getLogger(__name__)
paths = PathsManager()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_all_site_info() -> None:

    site_list = (cg.get_task_configs()['site_tasks']).keys()
    for site in site_list:
        logger.info(f'Writing info TOA5 file for site {site}')
        try:
            write_site_info(site=site)
            logger.info('... done')
        except FileNotFoundError:
            logger.error(
                f'Write of details file for site {site} failed with the '
                'following error: ',
                exc_info=True
                )
            continue
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_site_info(site: str) -> None:

    LOGGER_SUBSET = [
        'format', 'station_name', 'logger_type', 'serial_num', 'OS_version',
        'program_name'
        ]

    # Get site info
    logger.info('Getting site information...')
    site_info = cg.get_site_details_configs(site=site)
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
    fast_file_info = {'10Hz_file': mh.get_last_10Hz_file(site=site)}

    # Get flux logger info
    logger.info('Getting flux logger information...')
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
    output_path = paths.get_local_stream_path(
        resource='visualisation', stream='site_details', site=site
        )

    # Write to file
    logger.info('Writing to file')
    io.write_data_to_file(
        headers=headers, data=data, abs_file_path=output_path, info=info)
#------------------------------------------------------------------------------
