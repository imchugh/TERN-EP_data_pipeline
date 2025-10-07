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
import csv
import datetime as dt
import hashlib
import logging
import numpy as np
import pandas as pd
import pathlib

#------------------------------------------------------------------------------

from file_handling import read_cs_files as rcf
from managers import paths
from managers import site_details

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

INFO_NAMES = [
    'format', 'station_name', 'logger_type', 'serial_num', 'OS_version',
    'program_name', 'program_sig'
    ]
CSV_SEP = ','
CSV_QUOTING = 2
CSV_NA_VALUES = 'NAN'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

logger = logging.getLogger(__name__)
details = site_details.SiteDetailsManager()

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN CLASSES ###
###############################################################################

#------------------------------------------------------------------------------
class TOB3DataMonitor():
    """"""

    #--------------------------------------------------------------------------
    def __init__(self, site: str) -> None:
        """
        Initialise class.

        Args:
            site: name of site.

        Returns:
            None.

        """

        self.site = site
        self.interval = int(1000 /
            details.get_single_site_details(
                site=site,
                field='freq_hz'
                )
            )
        self.base_path = paths.get_local_stream_path(
            resource='raw_data',
            stream='flux_fast',
            site=site,
            subdirs=['TOB3']
            )
        self.file_list = list(self.base_path.rglob('TOB3*.dat'))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_first_file(self) -> str:
        """Get the first available file on the server"""

        return sorted(file.name for file in self.file_list)[0]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_last_file(self) -> str:
        """Get the last available file on the server"""

        return sorted(file.name for file in self.file_list)[-1]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_expected_files(self) -> list:
        """
        Generate file names for all possible files expected between beginning
        and end.

        Returns:
            list of expected files.

        """

        start_str = f'TOB3_{self.site}_100ms_'
        return [
            start_str +
            this_date.strftime('%Y_%m_%d.dat') for this_date in
            pd.date_range(
                start=self._convert_date(file_name=self.get_first_file()),
                end=self._convert_date(file_name=self.get_last_file()),
                freq='D'
                )
            ]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_missing_files(self) -> list:
        """
        Compare available and expected and return those missing.

        Returns:
            list of missing files.

        """

        available_files = [file.name for file in self.file_list]
        expected_files = self._get_expected_files()
        return sorted(list(set(expected_files) - set(available_files)))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _convert_date(self, file_name: str) -> dt.date:
        """
        Return date implied by date elements embedded in file name string.

        Args:
            file_name: name of file.

        Returns:
            date.

        """

        return (
            dt.datetime.strptime(
                '_'.join((file_name.split('.')[0]).split('_')[-3:]),
                '%Y_%m_%d'
                )
            .date()
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class FastDataConverter():
    """Simple class to convert data to TOA5"""

    #--------------------------------------------------------------------------
    def __init__(self, file: pathlib.Path | str) -> None:
        """
        Set attrs.

        Args:
            file: absolute path for input file.

        Raises:
            RuntimeError: raised if no data returned.

        Returns:
            None.

        """

        # Unpack and load the contents
        contents = rcf.read_cs_files(filename=file)
        if len(contents[0]) == 0:
            raise RuntimeError('No valid data discovered in file!')

        # Set basic attrs
        metadata = contents[1]
        self.file = file
        self.input_format = metadata[0][0]
        self.ex_meta = None
        if self.input_format == 'TOB3':
            self.ex_meta = metadata.pop(1)
        self.metadata = metadata

        # Pull the data into the dataframe and create headers from metadata
        # (note that there is an extra line of metadata for TOB3 at pos 1)
        data = (
            pd.DataFrame(dict(zip(self.metadata[1], contents[0])))
            .set_index(keys='TIMESTAMP' )
            )

        # For float cols:
        # 1) Downcast any columns to integer if no information lost (e.g. diags)
        # 2) Downcast float64 to float32 and round to prevent recast to 64 on
        #    csv output
        for col in data.select_dtypes('float'):
            if np.all(np.isclose(data[col], data[col].astype(int))):
                data[col] = data[col].astype(int)
            else:
                data[col] = (
                    data[col]
                    .astype('float32')
                    .apply(lambda x: float(f'{x:.7g}'))
                    )

        # Assign as attr
        self.data = data[~data.index.duplicated()].sort_index()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_header(self) -> pd.DataFrame:
        """
        Get the header as a dataframe.

        Returns:
            the dataframe.

        """

        return (
            pd.DataFrame(
                data=self.metadata[1:4],
                index=['variable', 'units', 'sampling']
                )
            .T
            .set_index(keys='variable')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_info(self) -> dict:
        """
        Get the pre-header info line as a dict.

        Returns:
            the dict.

        """

        return get_info_line(file=self.file, as_dict=True)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_data_to_file(self, output_file: pathlib.Path | str) -> None:
        """
        Write the data to an external file.

        Args:
            output_file (pathlib.Path | str): DESCRIPTION.

        Returns:
            None: DESCRIPTION.

        """

        # Write out the data
        _write_data_to_file(
            file=output_file,
            headers=_format_output_header(
                info=self.get_file_info(), header=self.get_file_header()
                ),
            data=_format_output_data(data=self.data)
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class DailyFastDataConverter(FastDataConverter):

    #--------------------------------------------------------------------------
    def __init__(
        self, file: pathlib.Path | str, time_step: int, freq_hz: int
        ) -> None:
        """
        Inherit from FastDataConverter and assign new attributes.

        Args:
            file (pathlib.Path | str): DESCRIPTION.
            time_step (int): DESCRIPTION.
            freq_hz (int): DESCRIPTION.

        Returns:
            None: DESCRIPTION.

        """

        super().__init__(file)
        self.time_step = time_step
        self.freq_hz = freq_hz
        self.n_expected = time_step * freq_hz * 60
        self.file_reference = self._make_df()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_df(self) -> pd.DataFrame:
        """
        Create a reference dataframe containing the dates and info required to
        partition into smaller file blocks.

        Returns:
            the dataframe.

        """

        # Get start and end dates for files at specified time step
        file_start = self.data.index[0]
        day_start = dt.datetime(
            file_start.year,
            file_start.month,
            file_start.day
            )
        day_end = day_start + dt.timedelta(days=1)

        dates = (
            pd.date_range(
                start=day_start,
                end=day_end,
                freq=f'{self.time_step}min'
                )
            [:-1]
            )

        start_date = dates + dt.timedelta(
            microseconds=10**6 / self.freq_hz
            )
        end_date = dates + dt.timedelta(
            minutes=self.time_step, seconds=0, microseconds=0
            )

        # Find the number of valid records
        n_recs = []
        for date_pair in zip(start_date, end_date):
            n_recs.append(len(self.data.loc[date_pair[0]: date_pair[1]]))

        # Assemble and return the dataframe
        return pd.DataFrame(
            data={
                'start_date': start_date,
                'end_date': end_date,
                'n_recs': n_recs,
                },
            index=pd.Index(data=range(len(dates)), name='file_num')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_by_dates(
        self, start: dt.datetime, end: dt.datetime
        ) -> pd.DataFrame:
        """
        Return a date-bound subset of the complete data.

        Args:
            start: date of first record.
            end: date of last record (inclusive).

        Returns:
            the subset dataframe.

        """

        return self.data.loc[start: end]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_by_file_num(self, num: int) -> pd.DataFrame:
        """
        Return a subset of the data based on the allocated file number
        (documented in file_reference dataframe).

        Args:
            num: the numerical reference of the data subset.

        Raises:
            RuntimeError: raised if no data in the file.

        Returns:
            the subset dataframe.

        """

        rec = self.file_reference.loc[num]
        if rec.n_recs == 0:
            start = rec.start_date.strftime('%H:%M')
            end = rec.end_date.strftime('%H:%M')
            raise RuntimeError(f'No data between {start} and {end}!')
        return self.data.loc[rec.start_date: rec.end_date]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_data_to_file_by_num(
            self, index_num: int, output_file: pathlib.Path | str
            ) -> None:
        """
        Write a subset of the data to file based on the allocated file number
        (documented in file_reference dataframe).

        Args:
            output_file (pathlib.Path | str): DESCRIPTION.

        Returns:
            None: DESCRIPTION.

        """

        # Write out the data
        _write_data_to_file(
            file=output_file,
            headers=_format_output_header(
                info=self.get_file_info(), header=self.get_file_header()
                ),
            data=_format_output_data(data=self.get_data_by_file_num(index_num))
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _format_output_header(info, header):

    # Move the header lines to a list
    header_list = []
    header_list.append(list(info.values()))
    header = header.reset_index()
    for item in header.columns:
        header_list.append(header[item].tolist())
    return header_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _format_output_data(data):

    # Turn bools into integer and round time stamps
    bool_data = data.select_dtypes(bool).astype(int)
    data = data.drop(bool_data.columns, axis=1)
    data = pd.concat([data, bool_data], axis=1).reset_index()
    data['TIMESTAMP'] = data['TIMESTAMP'].apply(_time_rounder)
    return data
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _time_rounder(timestamp):

    prec_us = 100000
    rounded = timestamp + dt.timedelta(microseconds=500)
    rounded.replace(microsecond=(rounded.microsecond // prec_us) * prec_us)
    tenths = rounded.microsecond // prec_us
    if tenths == 0:
        return f'{rounded:%Y-%m-%d %H:%M:%S}'
    return f'{rounded:%Y-%m-%d %H:%M:%S}.{tenths}'
#------------------------------------------------------------------------------

###############################################################################
### END CLASSES ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def file_fast_TOB3_data(site: str, is_aux=False) -> None:

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

    site_deets = details.get_single_site_details(site=site)
    today = dt.datetime.now().date()

    for file in sorted(base_path.rglob('TOB3*.dat')):

        # Get header lines, strip and check file type and date
        info = get_info_line(file=file, as_dict=True)
        if not info['format'] == 'TOB3':
            raise TypeError('Only implemented for files of type `TOB3`')
        if not info['station_name'] == site + '_EC':
            logger.warning('Warning: logger site name does not match site name')
        #     raise RuntimeError('Unexpected site name in data header!')
        creation_date = (
            dt.datetime.strptime(info['creation_date'], DATE_FORMAT).date()
            )
        if not creation_date < today:
            continue

        print('Parsed fast file! Done.')

        try:
            convert_fast_file(
                site=site,
                file=file,
                time_step=int(site_deets.time_step),
                freq=int(site_deets.freq_hz)
                )
            logger.info(f' - moving file {file.name}')
        except RuntimeError as e:
            logging.error(f'Error when unpacking TOB3 file: {e}')

        try:
            move_fast_file(
                site=site,
                in_file=file,
                freq=int(site_deets.freq_hz)
                )
            logging.info(' - ... Done')
        except FileExistsError as e:
            logging.error(f'Error when moving TOB3 file: {e}')
            continue
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_fast_file(
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

    logger.info(f' - parsing file {file.name}')

    # Read the file data into the fast data handler
    converter = DailyFastDataConverter(
        file=file, time_step=time_step, freq_hz=freq
        )

    # Get the output directory and ensure it exists
    out_dir = file.parents[1] / converter.date_start.strftime('TOA5/%Y_%m/%d')
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

    logger.info(' - ...Done!')
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
def move_fast_file(
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

    # Get date from file header (note the date stamp is when the file handle is
    # opened)
    with open(file=in_file, mode='rb') as f:
        date = dt.datetime.strptime(
            (
                f.readline()
                .decode()
                .strip()
                .split(',')[-1]
                ),
            '"%Y-%m-%d %H:%M:%S"'
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
                    'A non-identical file with that name already exists in the '
                    'destination directory!'
                    )

    # Move the file
    in_file.rename(out_file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_info_line(file: pathlib.Path | str, as_dict: bool=False):

    rslt = get_header_lines(file=file)[0]
    if not as_dict:
        return rslt
    if rslt[0] == 'TOB3':
        last_info_name = 'creation_date'
    elif rslt[0] == 'TOB1':
        last_info_name = 'table_name'
    info_keys = INFO_NAMES.copy()
    info_keys.append(last_info_name)
    return dict(zip(info_keys, rslt))

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_header_lines(file, n_lines=1):

    l = []
    with open(file=file, mode='rb') as f:
        for i, line in enumerate(f):
            l.append(line.decode().replace('"', '').strip().split(','))
            if i == n_lines - 1:
                return l
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_data_to_file(file, headers: list, data: pd.DataFrame) -> None:

    # Write the data to file
    with open(file, 'w', newline='\n') as f:

        # Write the header
        writer = csv.writer(
            f,
            delimiter=CSV_SEP,
            quoting=CSV_QUOTING
            )
        for row in headers:
            writer.writerow(row)

        # Write the data
        data.to_csv(
            f, header=False, index=False, na_rep=CSV_NA_VALUES,
            sep=CSV_SEP, quoting=CSV_QUOTING,
            )
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
    converter = FastDataConverter(file=input_file)

    # Write to file
    converter.write_data_to_file(output_file=output_file)
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
