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

import csv
import datetime as dt
import numpy as np
import pandas as pd
import pathlib

#------------------------------------------------------------------------------

from file_handling import read_cs_files as rcf

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

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN CLASSES ###
###############################################################################

# #------------------------------------------------------------------------------
# class FileConverter():
#     """Simple class to convert data to TOA5"""

#     #--------------------------------------------------------------------------
#     def __init__(self, file: pathlib.Path | str) -> None:
#         """
#         Set attrs.

#         Args:
#             file: absolute path for input file.

#         Raises:
#             RuntimeError: raised if no data returned.

#         Returns:
#             None.

#         """

#         # Unpack and load the contents
#         contents = rcf.read_cs_files(filename=file)
#         if len(contents[0]) == 0:
#             raise RuntimeError('No valid data discovered in file!')

#         # Set basic attrs
#         metadata = contents[1]
#         self.file = file
#         self.input_format = metadata[0][0]
#         self.ex_meta = None
#         if self.input_format == 'TOB3':
#             self.ex_meta = metadata.pop(1)
#         self.metadata = metadata

#         # Pull the data into the dataframe and create headers from metadata
#         # (note that there is an extra line of metadata for TOB3 at pos 1)
#         data = (
#             pd.DataFrame(dict(zip(self.metadata[1], contents[0])))
#             .set_index(keys='TIMESTAMP' )
#             )

#         # For float cols:
#         # 1) Downcast any columns to integer if no information lost (e.g. diags)
#         # 2) Downcast float64 to float32 and round to prevent recast to 64 on
#         #    csv output
#         for col in data.select_dtypes('float'):
#             if _check_integer_data(series=data[col]):
#                 try:
#                     data[col] = data[col].astype('Int32')
#                 except TypeError:
#                     data[col] = pd.to_numeric(
#                         data.Diag_CSAT, 
#                         downcast='integer', 
#                         errors='coerce'
#                         )
#             else:
#                 data[col] = (
#                     data[col]
#                     .astype('float32')
#                     .apply(lambda x: float(f'{x:.7g}'))
#                     )

#         # Assign as attr
#         self.data = data[~data.index.duplicated()].sort_index()
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_file_header(self) -> pd.DataFrame:
#         """
#         Get the header as a dataframe.

#         Returns:
#             the dataframe.

#         """

#         return (
#             pd.DataFrame(
#                 data=self.metadata[1:4],
#                 index=['variable', 'units', 'sampling']
#                 )
#             .T
#             .set_index(keys='variable')
#             )
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_file_info(self) -> dict:
#         """
#         Get the pre-header info line as a dict.

#         Returns:
#             the dict.

#         """

#         return get_info_line(file=self.file, as_dict=True)
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def write_data_to_file(self, output_file: pathlib.Path | str) -> None:
#         """
#         Write the data to an external file.

#         Args:
#             output_file (pathlib.Path | str): DESCRIPTION.

#         Returns:
#             None: DESCRIPTION.

#         """

#         # Write out the data
#         _write_data_to_file(
#             file=output_file,
#             headers=_format_output_header(
#                 info=self.get_file_info(), header=self.get_file_header()
#                 ),
#             data=_format_output_data(data=self.data)
#             )
#     #--------------------------------------------------------------------------

# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class FileConverter():

    #--------------------------------------------------------------------------
    def __init__(self, files: str |  list) -> None:
        
        # If files is a single file, convert to a list with single element
        if isinstance(files, str):
            files = [files]
        
        # Initialise and check file list
        self.file_list = sorted(pathlib.Path(file) for file in files)
        self._check_files_exist()
        
        # Get metadata list
        metadata = {
            file.name: rcf.read_cs_files(filename=file, metaonly=True)
            for file in self.file_list
            }
        self._set_format(metadata=metadata)
        
        # Get data
        data, start_file = self._get_data()
        
        # Assign metadata for the first chronological file
        metadata = metadata[start_file]
        self.ex_meta = None
        if self.input_format == 'TOB3':
            self.ex_meta = metadata.pop(1)
        self.metadata = metadata
        
        # For float cols:
        # 1) Downcast any columns to integer if no information lost (e.g. diags)
        # 2) Downcast float64 to float32 and round to prevent recast to 64 on
        #    csv output
        for col in data.select_dtypes('float'):
            if _check_integer_data(series=data[col]):
                try:
                    data[col] = data[col].astype('Int32')
                except TypeError:
                    data[col] = pd.to_numeric(
                        data.Diag_CSAT, 
                        downcast='integer', 
                        errors='coerce'
                        )
            else:
                data[col] = (
                    data[col]
                    .astype('float32')
                    .apply(lambda x: float(f'{x:.7g}'))
                    )

        # Assign as attr
        self.data = data[~data.index.duplicated()].sort_index()
    #--------------------------------------------------------------------------

    ###########################################################################
    ### PRIVATE (INIT) METHODS ###
    ###########################################################################

    #--------------------------------------------------------------------------
    def _check_files_exist(self):
        """
        

        Raises:
            FileNotFoundError: DESCRIPTION.

        Returns:
            None.

        """
        
        for file in self.file_list:
            if not file.exists():
                raise FileNotFoundError(f'File {file.name} does not exist')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _set_format(self, metadata):
        """
        

        Args:
            metadata (TYPE): DESCRIPTION.

        Raises:
            TypeError: DESCRIPTION.

        Returns:
            None.

        """
        
        rslt = np.unique([value[0][0] for value in metadata.values()])
        if len(rslt) != 1:
            raise TypeError('Files must all be of same format!')
        self.input_format = rslt[0].item()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_metadata(self, metadata):
        """
        Cross check all of the header lines to ensure they are the same.
        

        Raises:
            TypeError: DESCRIPTION.

        Returns:
            None.

        """
               
        # Check the format field in the header is consistent
        rslt = np.unique([value[0][0] for value in metadata.values()])
        if len(rslt) != 1:
            raise TypeError('Files must all be of same format!')
        input_format = rslt[0]
            
        # Check the complete info pre-header field is consistent
        self._check_meta_line(
            line_list=[info_line[0][:-1] for info_line in metadata.values()]
            )
        
        # Check the three main header lines 
        start_line = 1
        if input_format == 'TOB3':
            start_line += 1
        for i in range(start_line, start_line + 3):
            i += 1
            self._check_meta_line([value[i] for value in metadata.values()])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_meta_line(self, line_list):
        """
        

        Args:
            line_list (TYPE): DESCRIPTION.

        Raises:
            RuntimeError: DESCRIPTION.

        Returns:
            None.

        """
        
        if not all(lst == line_list[0] for lst in line_list):
            raise RuntimeError(
                'File header lines do not match!'
                )    
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------            
    def _get_data(self):
        """
        

        Returns:
            TYPE: DESCRIPTION.
            TYPE: DESCRIPTION.

        """
        
        header_line = 1
        if self.input_format == 'TOB3':
            header_line += 1
            
        file_dates = {}
        data_list = []
        for file in self.file_list:
            contents = rcf.read_cs_files(filename=file)
            metadata = contents[1]
            data = (
                pd.DataFrame(dict(zip(metadata[header_line], contents[0])))
                .set_index(keys='TIMESTAMP' )
                )
            file_dates[data.index[0].to_pydatetime()] = file.name
            data_list.append(data)
        return (
            pd.concat(data_list).sort_index().drop_duplicates(),
            pd.Series(file_dates).sort_index().tolist()[0]
            )       
    #--------------------------------------------------------------------------

    ###########################################################################
    ### PUBLIC METHODS ###
    ###########################################################################

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

        if self.input_format == 'TOB3':
            last_info_name = 'creation_date'
        elif self.input_format == 'TOB1':
            last_info_name = 'table_name'
        info_keys = INFO_NAMES.copy()
        info_keys.append(last_info_name)
        return dict(zip(info_keys, self.metadata[0]))
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
class DailyTOB3FileConverter(FileConverter):

    #--------------------------------------------------------------------------
    def __init__(
        self, files: pathlib.Path | str, time_step: int, freq_hz: int
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

        super().__init__(files)
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
    def get_data_audit(self):

        return self.file_reference
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



###############################################################################
### END CLASSES ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

# PRIVATE

#------------------------------------------------------------------------------
def _check_integer_data(series: pd.Series) -> bool:

    arr = series.to_numpy(dtype=float)
    mask = np.isclose(arr, np.round(arr), equal_nan=True)
    return bool(mask.all())
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

# PUBLIC

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

###############################################################################
### END FUNCTIONS ###
###############################################################################
