# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 09:53:52 2024
@author: jcutern-imchugh

This module handles concatenation of individual 24 hour files from Licor
SmartFlux systems into a single accumulation file. It consists of two functions:
    * write_to_eddypro_file - merge individual summary files and write as
        single master file.
    * append_to_eddypro_file - append new files to existing master file.
Note that this script calls the file_handler to deal with concatenation to the
existing file, rather than the more efficent option - just opening the file in
append mode and tacking on the data. This is because we do not have central
control of the content of the incoming EddyPro files. If the content of those
files changes and a naive concatenation runs, column assignment will be
corrupted. The file_handler does legality checks on the merge to ensure this
does not happen.
"""

#------------------------------------------------------------------------------
# STANDARD IMPORTS #
import logging
import os
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS #
import file_handling.file_handler as fh
import file_handling.file_io as io
from paths import paths_manager as pm
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# INITS #
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
        pm.get_local_stream_path(
            resource='raw_data', stream='flux_slow', site=site
            )
        )
    master_file = f'{site}_EP_MASTER.txt'
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
        pm.get_local_stream_path(
            resource='raw_data', stream='flux_slow', site=site
            )
        )
    output_file = f'{site}_EP_MASTER_temp.dat'
    files_to_append = sorted(data_path.glob(f'*{EP_SEARCH_STR}*.txt'))

    # Use the newest file as master, and previous files as append list
    master_file = max(files_to_append) #, key=os.path.getctime)
    master_idx = files_to_append.index(master_file)
    files_to_append = files_to_append[: master_idx]

    # Get the file handler and write to new master
    handler = fh.DataHandler(file=master_file, concat_files=files_to_append)
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
