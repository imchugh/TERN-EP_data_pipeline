#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 20 15:42:13 2025

Module to:
    1) process fast daily data files (using externally developed code repo
    from GitHub) in compressed format (TOB1 and TOB3 supported) format to TOA5;
    2) write out to individual files at the interval specified in the DB
    3) rename all files and transfer to date-based directory structure.

Consists of the following classes:
    FastDataConverter: class that converts compressed data.
    DailyFastDataConverter: takes a daily TOB3 fast data file as input, converts
    the data and holds it as a class attr. Allows the fast data to be subsetted
    into files of EddyPro-ready length.

To do:
    *

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import argparse
import datetime as dt
import hashlib
import logging
import pathlib

#------------------------------------------------------------------------------

from utils import fast_file_io as ffio
from managers import paths
from managers import site_details

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

logger = logging.getLogger(__name__)
details = site_details.SiteDetailsManager()

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def parse_TOB3_daily(site: str, is_aux=False) -> None:
    """


    Args:
        site (str): DESCRIPTION.
        is_aux (TYPE, optional): DESCRIPTION. Defaults to False.

    Returns:
        None: DESCRIPTION.

    """

    logger.info('Converting fast TOB3 files:')

    stream = 'flux_fast'
    if is_aux:
        stream += '_aux'

    base_path = paths.get_local_stream_path(
        resource='raw_data',
        stream=stream,
        site=site,
        subdirs=['TMP']
        )

    # Get required metadata
    site_deets = details.get_single_site_details(site=site)
    time_step = int(site_deets.time_step)
    freq = int(site_deets.freq_hz)
    today = dt.datetime.now().date()

    # Iterate over files in directory
    for file in sorted(base_path.rglob('TOB3*.dat')):

        # Get header lines and check file type, station and date
        info = ffio.get_info_line(file=file, as_dict=True)
        if not info['format'] == 'TOB3':
            logger.error(f'File `{file.name}` is not a TOB3 file! Skipping...')
        if not info['station_name'] == site + '_EC':
            logger.warning('Warning: logger site name does not match site name')
        creation_date = _convert_date_str(date_str=info['creation_date'])
        if not creation_date < today:
            continue

        # Convert the data
        try:
            convert_TOB3_daily(
                site=site, file=file, time_step=time_step, freq=freq
                )
        except RuntimeError as e:
            logging.error(f'Error when unpacking TOB3 file: {e}')

        # Move the data
        try:
            move_TOB3_daily(
                site=site,
                in_file=file,
                freq=int(site_deets.freq_hz)
                )
            logging.info(' - Done')
        except FileExistsError as e:
            logging.error(f'Error when moving TOB3 file: {e}')
            continue
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_TOB3_daily(
        site: str, file: pathlib.Path, time_step=int, freq=int
        ) -> None:
    """
    Convert the data for a daily file and parcel out to the averaging period of
    the site.

    Args:
        site: name of site.
        file: path to file.
        time_step: averaging period of the site.
        freq: fast data collection frequency.

    Raises:
        RunTimeError: raised if no data between time stamps.

    Returns:
        None.

    """

    logger.info(f' - unpacking file {file.name}')

    # Read the file data into the fast data handler
    converter = ffio.DailyTOB3FileConverter(
        file=file, time_step=time_step, freq_hz=freq
        )

    # Get the output directory and ensure it exists
    date = _convert_date_str(converter.get_file_info()['creation_date'])
    out_dir = file.parents[1] / date.strftime('TOA5/%Y_%m/%d')
    out_dir.mkdir(parents=True, exist_ok=True)

    # Create the prefix for the file name
    freq_str = str(int(1000 / freq)) + 'ms'
    out_file_prefix = f'TOA5_{site}_{freq_str}'

    # Iterate over subfiles
    for num in converter.file_reference.index:

        # Get the date of the file and create the complete outfile name
        date = converter.file_reference.loc[num, 'end_date']
        out_file_suffix = date.strftime('_%Y_%m_%d_%H_%M.dat')
        output_file = out_dir / (out_file_prefix + out_file_suffix)

        logger.info(
            f'    Writing out {time_step} minute file ending with date '
            f'{date.strftime("%H:%M")}'
            )

        # Subset the data (will raise a RunTimeError if no data)
        try:
            converter.write_data_to_file_by_num(
                index_num=num, output_file=output_file
                )
        except RuntimeError as e:
            logger.error(f'Encountered an error: {e}')
            continue

    logger.info(' - Done!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def move_TOB3_daily(
        site: str, in_file: pathlib.Path, freq: int, check_before_move=True
        ) -> None:
    """
    Move and rename the existing fast file as it was collected to year_month
    file structure.

    Args:
        site: name of site.
        in_file: file to be moved.
        freq: fast data collection frequency.

    Returns:
        None.

    """

    logger.info(f' - moving file `{in_file.name}`...')

    # Get date from file header (note the date stamp is when the file handle is
    # opened)
    date = _convert_date_str(
        date_str=ffio.get_info_line(file=in_file, as_dict=True)['creation_date']
        )

    # Get the output directory and ensure it exists
    out_dir = in_file.parents[1] / date.strftime('TOB3/%Y_%m')
    out_dir.mkdir(parents=True, exist_ok=True)

    # Get the output file name
    freq_str = str(int(1000 / freq)) + 'ms'
    file_date_str = date.strftime('%Y_%m_%d')
    out_file = out_dir / f'TOB3_{site}_{freq_str}_{file_date_str}.dat'

    # Check the file exists
    if check_before_move:
        if out_file.exists():
            if not get_file_hash(file=in_file) == get_file_hash(file=out_file):
                raise FileExistsError(
                    f'A non-identical file with the name `{out_file.name}` '
                    'already exists in the destination directory!'
                    )

    # Move the file
    in_file.rename(out_file)
    logger.info(f' - successfully moved and renamed file to {out_file.name}')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _convert_date_str(date_str):

    return dt.datetime.strptime(date_str, ffio.DATE_FORMAT).date()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_file_hash(file: pathlib.Path | str) -> str:
    """
    Do the checksum for the file.

    Args:
        file: absolute file path.

    Returns:
        the hash.

    """

    with open(file, 'rb') as f:
        the_bytes=f.read()
        return hashlib.sha256(the_bytes).hexdigest()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_file(
        input_file: pathlib.Path | str, output_file: pathlib.Path | str,
        overwrite: bool=False
        ) -> None:
    """
    Convert from a compressed Campbell TOB3 or TOB1 file to Campbell TOA5.

    Args:
        input_file: source (compressed) file.
        output_file: target (uncompressed) file.
        overwrite: set True to overwrite existing output file of same name.

    Raises:
        RuntimeError: raised if input and output files have same name.
        FileNotFoundError: raised if the parent directory does not exist.
        FileExistsError: raised if the output file exists and the overwrite
        Flag is set to False.

    Returns:
        None.

    """

    # Check file inputs
    input_file = pathlib.Path(input_file)
    output_file = pathlib.Path(output_file)
    if input_file == output_file:
        raise RuntimeError('Input and output file cannot be the same!')
    if not input_file.exists():
        raise FileNotFoundError(
            f'File of name {input_file.name} in directory {input_file.parent} '
            'does not exist!'
            )
    if not output_file.parent.exists():
        raise FileNotFoundError(
            f'Specified output directory {output_file.parent} does not exist!'
            )
    if not overwrite:
        if output_file.exists():
            raise FileExistsError(
                'Overwrite of existing file must be explicitly permitted by '
                'setting `overwrite` arg to True!'
                )

    # Get the data converter
    converter = ffio.FileConverter(file=input_file)

    # Write to file
    converter.write_data_to_file(output_file=output_file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_last_fast_file(site, abs_path=True):
    """
    Get the most recent file that has been named and filed

    Args:
        site (TYPE): DESCRIPTION.
        abs_path (TYPE, optional): DESCRIPTION. Defaults to True.

    Raises:
        FileNotFoundError: DESCRIPTION.

    Returns:
        TYPE: DESCRIPTION.

    """

    
    search_dir = paths.get_local_stream_path(
        resource='raw_data',
        stream='flux_fast',
        site=site,
        )
    try:
        rslt = (
            sorted(
                [
                    x for x in search_dir.rglob('TOB3_*.dat') 
                    if x.parents[1] !='TMP'
                    ]
                )
            [-1]
            )
    except IndexError:
        raise FileNotFoundError('No files')
    if abs_path:
        return rslt
    return rslt.name
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################

if __name__ == '__main__':

    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        'input_file', help='Input compressed file for conversion'
        )
    argparser.add_argument(
        'output_file', help='Output file for converted data'
        )
    argparser.add_argument(
        '--overwrite', action='store_true',
        help='pass `overwrite=True` if allowing existing file overwrite'
        )
    args = argparser.parse_args()
    convert_file(
        input_file=args.input_file,
        output_file=args.output_file,
        overwrite=args.overwrite
        )
