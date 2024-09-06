# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 11:52:46 2023

@author: jcutern-imchugh
"""


import numpy as np
import pandas as pd
import pathlib

import file_handling.file_io as io
import file_handling.file_concatenators as fc

###############################################################################
### CLASSES ###
###############################################################################

#------------------------------------------------------------------------------
class DataHandler():

    #--------------------------------------------------------------------------
    def __init__(
            self, file: pathlib.Path | str, concat_files:bool=False
            ) -> None:
        """
        Set attributes of handler.

        Args:
            file: absolute path to file for which to create the handler.
            concat_files (optional): if false, the content of the passed file
                is parsed in isolation. If true, any available backup (TOA5)
                or string-matched EddyPro files stored in the same directory
                are concatenated. If list, the files contained therein will be
                concatenated with the main file. Defaults to False.

        Returns:
            None.

        """

        rslt = _get_handler_elements(file=file, concat_files=concat_files)
        for key, value in rslt.items():
            setattr(self, key, value)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_conditioned_data(self,
            usecols: list | dict=None, output_format: str=None,
            drop_non_numeric: bool=False, monotonic_index: bool=False,
            resample_intvl: int=None, raise_if_dupe_index: bool=False
            ) -> pd.DataFrame:
        """
        Condition and output a copy of the underlying data.

        Args:
            usecols (optional): the list of columns to include in the output.
                If a dict is passed, the columns are renamed, with the mapping
                from existing to new defined by the key (old name): value
                (new_name) pairs. Defaults to None.
            output_format (optional): format for data output (None, TOA5 or
                EddyPro).Defaults to None.
            drop_non_numeric (optional): purge the non-numeric columns from the
                conditioned data. If false, the non-numeric columns will be
                included even if excluded from usecols. If true they will be
                dropped even if included in usecols. Defaults to False.
            monotonic_index (optional): align the data to a monotonic index.
                Defaults to False.
            resample_intvl (optional): time interval to which the data should
                be resampled. Defaults to None.
            raise_if_dupe_index (optional): raise an error if duplicate indices
                are found with non-duplicate data. Defaults to False.

        Raises:
            RuntimeError: raised if duplicate indices are found with
                non-duplicate data.

        Returns:
            Copy of underlying dataframe with altered data.

        """

        # Apply column subset and rename
        subset_list, rename_dict = self._subset_or_translate(usecols=usecols)
        output_data = self.data[subset_list].rename(rename_dict, axis=1)

        # Apply duplicate mask
        dupe_records = self.get_duplicate_records()
        dupe_indices = self.get_duplicate_indices()
        if any(dupe_indices) and raise_if_dupe_index:
            raise RuntimeError(
                'Duplicate indices with non-duplicate data!'
                )
        dupes_mask = dupe_indices | dupe_records
        output_data = output_data.loc[~dupes_mask]

        # Do the resampling
        if monotonic_index and not resample_intvl:
            resample_intvl = f'{self.interval}T'
        if resample_intvl:
            output_data = output_data.resample(resample_intvl).asfreq()

        # If platform-specific formatting not requested, drop non-numerics
        # (if requested) and return
        if output_format is None:
            if drop_non_numeric:
                for var in self._configs['non_numeric_cols']:
                    try:
                        output_data.drop(var, axis=1, inplace=True)
                    except KeyError:
                        pass
            return output_data

        # Format and return data
        return io.reformat_data(
                data=output_data,
                output_format=output_format
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_conditioned_headers(
            self, usecols=None, output_format=None, drop_non_numeric=False
            ):
        """


        Args:
            usecols (TYPE, optional): DESCRIPTION. Defaults to None.
            output_format (TYPE, optional): DESCRIPTION. Defaults to None.
            drop_non_numeric (TYPE, optional): DESCRIPTION. Defaults to False.

        Returns:
            TYPE: DESCRIPTION.

        """
        """


        Parameters
        ----------
        usecols : list or dict, optional
            The columns to include in the output. If a dict is passed, then
            the columns are renamed, with the mapping from existing to new
            defined by the key (old name): value (new_name) pairs.
            The default is None.
        output_format : str, optional
            The format for header output (None, TOA5 or EddyPro).
            The default is None.
        drop_non_numeric : bool, optional
            Purge the non-numeric headers from the conditioned data. If false,
            the non-numeric headers will be included even if excluded from
            usecols. If true they will be dropped even if included in usecols.
            The default is False.

        Returns
        -------
        output_headers : pd.core.frame.DataFrame
            Dataframe with altered headers.

        """

        # Apply column subset and rename
        subset_list, rename_dict = self._subset_or_translate(usecols=usecols)
        output_headers = self.headers.loc[subset_list].rename(rename_dict)

        # If platform-specific formatting not requested, drop non-numerics
        # (if requested) and return
        if output_format is None:
            if drop_non_numeric:
                for var in self._configs['non_numeric_cols']:
                    try:
                        output_headers.drop(var, inplace=True)
                    except KeyError:
                        pass
            return output_headers

        # Format and return data
        return io.reformat_headers(
            headers=output_headers,
            output_format=output_format
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_duplicate_records(self, as_dates=False):
        """
        Get a representation of duplicate records (boolean pandas Series).

        Parameters
        ----------
        as_dates : bool, optional
            Output just the list of dates for which duplicate records occur.
            The default is False.

        Returns
        -------
        series or list
            Output boolean series indicating duplicates, or list of dates.

        """

        records = self.data.reset_index().duplicated().set_axis(self.data.index)
        if as_dates:
            return self.data[records].index.to_pydatetime().tolist()
        return records
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_duplicate_indices(self, as_dates=False):
        """
        Get a representation of duplicate indices (boolean pandas Series).

        Parameters
        ----------
        as_dates : bool, optional
            Output just the list of dates for which duplicate indices occur.
            The default is False.

        Returns
        -------
        series or list
            Output boolean series indicating duplicates, or list of dates.

        """

        records = self.get_duplicate_records()
        indices = ~records & self.data.index.duplicated()
        if as_dates:
            return self.data[indices].index.to_pydatetime().tolist()
        return indices
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_missing_records(self, raise_if_single_record=False):
        """
        Get simple statistics for missing records

        Returns
        -------
        dict
            Dictionary containing the number of missing cases, the % of missing
            cases and the distribution of gap sizes.

        """

        data = self._get_non_duplicate_data()
        complete_index = pd.date_range(
            start=data.index[0],
            end=data.index[-1],
            freq=f'{self.interval}T'
            )
        n_missing = len(complete_index) - len(data)
        return {
            'n_missing': n_missing,
            'pct_missing': round(n_missing / len(complete_index) * 100, 2),
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_gap_bounds(self):

        data, gap_series = self._init_gap_analysis()
        return pd.concat(
            [
                gap_series.reset_index().n_records,
                pd.DataFrame(
                    data=[
                        data.iloc[x-1: x+1].astype(str).tolist()
                        for x in gap_series.index
                        ],
                    columns=['last_preceding', 'first_succeeding']
                    )
                ],
            axis=1
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_gap_distribution(self):

        gap_series = self._init_gap_analysis()[1]
        unique_gaps = gap_series.unique()
        counts = [len(gap_series[gap_series==x]) for x in unique_gaps]
        return (
            pd.DataFrame(
                data=zip(unique_gaps - 1, counts),
                columns=['n_records', 'count']
                )
            .set_index(keys='n_records')
            .sort_index()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _init_gap_analysis(self):

        # If file is TOA5, it will have a column AND an index named TIMESTAMP,
        # so dump the variable if so
        data = self._get_non_duplicate_data()
        try:
            data = data.drop('TIMESTAMP', axis=1)
        except KeyError:
            pass

        # Get instances of duplicate indices OR records, and remove
        data = data.reset_index()['DATETIME']

        # Get gaps as n_records (exclude gaps equal to measurement interval!)
        gap_series = (
            (data - data.shift())
            .astype('timedelta64[s]')
            .replace(self.interval, np.nan)
            .dropna()
            .apply(lambda x: x.total_seconds() / (60 * self.interval))
            .astype(int)
            .rename('n_records')
            )
        gap_series = gap_series[gap_series!=1]
        return data, gap_series
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_non_duplicate_data(self):

        if self.interval is None:
            raise TypeError('Analysis not applicable to single record!')
        dupes = self.get_duplicate_indices() | self.get_duplicate_records()
        return self.data[~dupes]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_date_span(self):
        """
        Get start and end dates

        Returns
        -------
        dict
            Dictionary containing start and end dates (keys "start" and "end").

        """

        return {
            'start_date': self.data.index[0].to_pydatetime(),
            'end_date': self.data.index[-1].to_pydatetime()
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_non_numeric_variables(self):

        return self._configs['non_numeric_cols']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_numeric_variables(self):

        return [
            col for col in self.data.columns if not col in
            self.get_non_numeric_variables()
            ]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_list(self):
        """
        Gets the list of variables in the TOA5 header line

        Returns
        -------
        list
            The list.

        """

        return self.headers.index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_units(self, variable):
        """
        Gets the units for a given variable

        Parameters
        ----------
        variable : str
            The variable for which to return the units.

        Returns
        -------
        str
            The units.

        """

        return self.headers.loc[variable, 'units']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_variable_units(self):
        """
        Get a dictionary cross-referencing all variables in file to their units

        Returns
        -------
        dict
            With variables (keys) and units (values).

        """

        return dict(zip(
            self.headers.index.tolist(),
            self.headers.units.tolist()
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_sampling_units(self):
        """
        Get a dictionary cross-referencing all variables in file to their
        sampling methods

        Returns
        -------
        dict
            With variables (keys) and sampling (values).

        """

        if self.file_type == 'EddyPro':
            raise NotImplementedError(
                f'No station info available for file type "{self.file_type}"')
        return dict(zip(
            self.headers.index.tolist(),
            self.headers.sampling.tolist()
            ))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_concatenation_report(self, abs_file_path):
        """
        Write the concatenation report to file.

        Parameters
        ----------
        abs_file_path : str or pathlib.Path
            Absolute path to the file.

        Raises
        ------
        TypeError
            Raised if no concatenated files.

        Returns
        -------
        None.

        """

        if not self.concat_report:
            raise TypeError(
                'Cannot write a concatenation report if there are no '
                'concatenated files!'
                )
        fc._write_text_to_file(
            line_list=self.concat_report,
            abs_file_path=abs_file_path
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_conditioned_data(
            self, abs_file_path, usecols=None, output_format=None,
            drop_non_numeric=False, **kwargs
            ):

        if output_format is None:
            output_format = self.file_type

        io.write_data_to_file(
            headers=self.get_conditioned_headers(
                usecols=usecols,
                output_format=output_format,
                drop_non_numeric=drop_non_numeric,
                ),
            data=self.get_conditioned_data(
                usecols=usecols,
                output_format=output_format,
                drop_non_numeric=drop_non_numeric,
                **kwargs
                ),
            abs_file_path=abs_file_path,
            output_format=output_format,
            info=self.file_info
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _subset_or_translate(self, usecols):

        # Set the subsetting list and rename dict, depending on type
        if usecols is None:
            subset_list, rename_dict = self.data.columns, {}
        elif isinstance(usecols, dict):
            subset_list, rename_dict = list(usecols.keys()), usecols.copy()
        elif isinstance(usecols, list):
            subset_list, rename_dict = usecols.copy(), {}
        else:
            raise TypeError('usecols arg must be None, list or dict')

        # Return the subset list and the renaming dictionary
        return subset_list, rename_dict
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------



###############################################################################
### BEGIN INIT FUNCTIONS ###
###############################################################################



#------------------------------------------------------------------------------
def _get_handler_elements(file, concat_files=False):
    """
    Get elements required to populate file handler for either single file or
    multi-file concatenated data.

    Parameters
    ----------
    file : str or pathlib.Path
        Absolute path to master file.
    concat_files : boolean or list
        See concat_files description in __init__ docstring for DataHandler.

    Returns
    -------
    dict
        Contains data (key 'data'), headers (key 'headers') and concatenation
        report (key 'concat_report').

    """

    # Set an emptry concatenation list
    concat_list = []

    # If boolean passed...
    if concat_files is True:
        concat_list = io.get_eligible_concat_files(file=file)

    # If list passed...
    if isinstance(concat_files, list):
        concat_list = concat_files

    # If concat_list has no elements, get single file data
    if len(concat_list) == 0:
        fallback = False if not concat_files else True
        data_dict = _get_single_file_data(file=file, fallback=fallback)

    # If concat_list has elements, use the concatenator
    if len(concat_list) > 0:
        data_dict = _get_concatenated_file_data(
            file=file,
            concat_list=concat_list
            )

    # Get file interval regardless of provenance (single or concatenated)
    data_dict.update(
        {'interval': io.get_datearray_interval(
            datearray=np.array(data_dict['data'].index.to_pydatetime())
            )
            }
        )

    # Return the dictionary
    return data_dict
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_concatenated_file_data(file, concat_list):

    file_type = io.get_file_type(file=file)
    configs = io.get_file_type_configs(file_type=file_type)
    concatenator = fc.FileConcatenator(
        master_file=file,
        file_type=file_type,
        concat_list=concat_list
        )
    return {
        'file_type': file_type,
        'file_info': io.get_file_info(
            file=file, file_type=file_type, dummy_override=True
            ),
        'data': concatenator.get_concatenated_data(),
        'headers': concatenator.get_concatenated_headers(),
        'concat_list': concat_list,
        'concat_report': concatenator.get_concatenation_report(as_text=True),
        '_configs': configs
        }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_single_file_data(file, fallback=False):

    file_type = io.get_file_type(file=file)
    configs = io.get_file_type_configs(file_type=file_type)
    return {
        'file_type': file_type,
        'file_info': io.get_file_info(file=file, file_type=file_type),
        'data': io.get_data(file=file, file_type=file_type),
        'headers': io.get_header_df(file=file, file_type=file_type),
        'concat_list': [],
        'concat_report': [] if not fallback else ['No eligible files found!'],
        '_configs': configs
        }
#------------------------------------------------------------------------------

###############################################################################
### END INIT FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN PUBLIC FUNCTIONS ###
###############################################################################

# #------------------------------------------------------------------------------
# def merge_data(
#         files: list | dict, concat_files: bool=False
#         ) -> pd.core.frame.DataFrame:
#     """
#     Merge and align data from different files.

#     Args:
#         files: the absolute path of the files to parse.
#         If a list, all variables returned; if a dict, file is value, and key
#         is passed to the file_handler. That key can be a list of variables, or
#         a dictionary mapping translation of variable names (see file handler
#         documentation).

#     Returns:
#         merged data.

#     """

#     df_list = []
#     for file in files:
#         try:
#             usecols = files[file]
#         except TypeError:
#             usecols = None
#         data_handler = DataHandler(file=file, concat_files=concat_files)
#         df_list.append(
#             data_handler.get_conditioned_data(
#                 usecols=usecols, drop_non_numeric=True,
#                 monotonic_index=True
#                 )
#             )
#     return (
#         pd.concat(df_list, axis=1)
#         .rename_axis('time')
#         )
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def merge_headers(
#         files: list | dict, concat_files: bool=False
#         ) -> pd.core.frame.DataFrame:
#     """
#     Merge and align data from different files.

#     Args:
#         files: the absolute path of the files to parse.
#         If a list, all variables returned; if a dict, file is value, and key
#         is passed to the file_handler. That key can be a list of variables, or
#         a dictionary mapping translation of variable names (see file handler
#         documentation).

#     Returns:
#         merged data.

#     """

#     df_list = []
#     for file in files:
#         try:
#             usecols = files[file]
#         except TypeError:
#             usecols = None
#         data_handler = DataHandler(file=file, concat_files=concat_files)
#         df_list.append(
#             data_handler.get_conditioned_headers(
#                 usecols=usecols, drop_non_numeric=True,
#                 )
#             )
#     return (
#         pd.concat(df_list)
#         .rename_axis('variable')
#         )
# #------------------------------------------------------------------------------

###############################################################################
### END PUBLIC FUNCTIONS ###
###############################################################################
