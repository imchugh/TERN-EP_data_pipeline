# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 09:13:12 2024

@author: jcutern-imchugh
"""

import pandas as pd
import pathlib

from file_handling import file_handler as fh
from file_handling import file_io as io
from data_conditioning import data_filtering as dtf
from data_conditioning import data_convs_calcs as dtc
import utils.metadata_handlers as mh

#------------------------------------------------------------------------------
class data_merger():

    #--------------------------------------------------------------------------
    def __init__(self, site, variable_map='pfp', concat_files=True):

        self.site = site
        self.md_mngr = mh.MetaDataManager(site=site, variable_map='vis')
        merge_dict = {
            file: self.md_mngr.translate_variables_by_table(table=table)
            for table, file in self.md_mngr.map_tables_to_files(abs_path=True).items()
            }
        rslt = merge_all(files=merge_dict, concat_files=concat_files)
        self.data = rslt['data']
        self.headers = rslt['headers']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data(self, convert_units=True, apply_limits=True):

        output_data = self.data.copy()

        # Convert from site-based to standard units (if requested and if different)
        if convert_units:
            self._convert_units(output_data)

        # Apply range limits (if requested)
        if apply_limits:
            self._apply_limits(df=output_data)

        return output_data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _convert_units(self, df):

        for variable in self.md_mngr.list_variables_for_conversion():

            attrs = self.md_mngr.get_variable_attributes(variable)
            func = dtc.convert_variable(variable=variable)
            df[variable] = func(df[variable], from_units=attrs['units'])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _apply_limits(self, df):

        for variable in self.md_mngr.list_variables():

            attrs = self.md_mngr.get_variable_attributes(variable)
            df[variable] = dtf.filter_data(
                series=df[variable],
                max_val=attrs['plausible_max'],
                min_val=attrs['plausible_min']
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_headers(self, convert_units=True):

        output_headers = self.headers.copy()
        if convert_units:
            output_headers['units'] = (
                self.md_mngr.variable_lookup_table.standard_units
                .reindex(output_headers.index)
                )
        return output_headers
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_file(
            self, path_to_file: pathlib.Path | str=None,
            convert_units: bool=True, apply_limits: bool=True) -> None:

        if not path_to_file:
            path_to_file = (
                self.md_mngr.data_path / f'{self.md_mngr.site}_merged.dat'
                )

        headers = self.get_headers(convert_units=convert_units)
        data = self.get_data(
            convert_units=convert_units,
            apply_limits=apply_limits
            )
        io.write_data_to_file(
            headers=io.reformat_headers(headers=headers, output_format='TOA5'),
            data=io.reformat_data(data=data, output_format='TOA5'),
            abs_file_path=path_to_file,
            output_format='TOA5'
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------
def merge_all(
        files: list | dict, concat_files: bool=False
        ) -> pd.core.frame.DataFrame:
    """
    Merge and align data and headers from different files.

    Args:
        files: the absolute path of the files to parse.
        If a list, all variables returned; if a dict, file is value, and key
        is passed to the file_handler. That key can be a list of variables, or
        a dictionary mapping translation of variable names (see file handler
        documentation).

    Returns:
        merged data.

    """

    data_list, header_list = [], []
    for file in files:
        try:
            usecols = files[file]
        except TypeError:
            usecols = None
        data_handler = fh.DataHandler(file=file, concat_files=concat_files)
        data_list.append(
            data_handler.get_conditioned_data(
                usecols=usecols, drop_non_numeric=True,
                monotonic_index=True
                )
            )
        header_list.append(
            data_handler.get_conditioned_headers(
                usecols=usecols, drop_non_numeric=True
                )
            )
    return {
        'headers': pd.concat(header_list),
        'data': pd.concat(data_list, axis=1)
        }
#------------------------------------------------------------------------------

# if __name__=='__main__':

#     make_file(site='Calperum')