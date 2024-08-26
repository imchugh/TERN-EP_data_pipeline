# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 09:53:52 2024
@author: jcutern-imchugh

This module handles concatenation of individual 24 hour files from Licor
SmartFlux systems into a single accumulation file. It consists of two functions:
    * write_to_eddypro_file - merge individual summary files and write as
        single master file.
    * append_to_eddypro_file - append new files to existing master file.

"""

#------------------------------------------------------------------------------
# STANDARD IMPORTS
import logging
import os
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS
import utils.configs_manager as cm
import file_handling.file_handler as fh
import file_handling.file_io as io
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# INITS
paths = cm.PathsManager()
EP_SEARCH_STR = 'EP-Summary'
EP_MASTER_FILE = 'EddyPro_master.txt'
logger = logging.getLogger(__name__)
#------------------------------------------------------------------------------



###############################################################################
### BEGIN EDDYPRO WRITE / APPEND FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def append_to_eddypro_file(site: str) -> None:
    """
    Append data to existing EddyPro master file.

    Args:
        site: name of site.

    Raises:
        FileNotFoundError: raised if no master file exists.

    Returns:
        None.

    """

    # Get the master file (raise error if missing)
    data_path = (
        paths.get_local_stream_path(
            resource='data', stream='flux_slow', site=site
            )
        )
    master_file = 'test.txt' # Change this to site-based naming
    if not (data_path / master_file).exists():
        raise FileNotFoundError('Concatenated EddyPro file does not exist!')

    # Get dates and log info
    master_dates = io.get_start_end_dates(file=data_path / master_file)
    logger.info(f'Found master EddyPro master file {master_file}')
    logger.info(f'Last record in master file is {master_dates["end_date"]}')

    # Get the individual files and iterate over dates to find missing
    files = list(data_path.glob(f'*{EP_SEARCH_STR}*.txt'))
    files_to_append = []
    for file in files:
        dates = io.get_start_end_dates(file=file)
        if dates['end_date'] > master_dates['end_date']:
            files_to_append.append(file)

    # If there are no new files, exit with logging error. Otherwise log.
    if len(files_to_append) == 0:
        logger.error('No new files to append!')
        return
    logger.info(
        'Appending the following files: {0}'
        .format(', '.join([file.name for file in files_to_append]))
        )

    # Get the file handler and write to new master
    handler = fh.DataHandler(
        file=data_path / master_file,
        concat_files=files_to_append
        )
    handler.write_conditioned_data(data_path / master_file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_to_eddypro_file(site: str) -> None:
    """
    Write individual files to a single master file.

    Args:
        site: name of site.

    Returns:
        None.

    """

    # Get the file list
    data_path = (
        paths.get_local_stream_path(
            resource='data', stream='flux_slow', site=site
            )
        )
    output_file = 'test.txt' # Change this to site-based naming
    files_to_append = list(data_path.parent.glob(f'*{EP_SEARCH_STR}*.txt'))

    # Use the newest file as master, and previous files as append list
    master_file = max(files_to_append, key=os.path.getctime)
    master_idx = files_to_append.index(master_file)
    files_to_append = files_to_append[: master_idx]

    # Get the file handler and write to new master
    handler = fh.DataHandler(file=output_file, concat_files=files_to_append)
    handler.write_conditioned_data(data_path / output_file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def update_eddypro_master(site):

    try:
        append_to_eddypro_file(site=site)
    except FileNotFoundError:
        write_to_eddypro_file(site=site)
#------------------------------------------------------------------------------

###############################################################################
### END EDDYPRO WRITE / APPEND FUNCTIONS ###
###############################################################################
