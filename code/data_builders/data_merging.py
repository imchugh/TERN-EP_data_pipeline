# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 09:13:12 2024

@author: jcutern-imchugh
"""

import pandas as pd
import pathlib

import data_conditioning.convert_calc_filter as ccf
import file_handling.file_handler as fh
import file_handling.file_io as io
import utils.metadata_handlers as mh
import utils.configs_manager as cm

#------------------------------------------------------------------------------
class data_merger():
    """Class to:
        1) merge variables from different sources;
        2) fill missing variables;
        3) apply broad range limits, and;
        4) output data to file.
    """

    #--------------------------------------------------------------------------
    def __init__(
            self, site: str, variable_map: str='pfp', concat_files: bool=True
            ) -> None:
        """
        Assign metadata manager, missing variables and raw data and headers.

        Args:
            site: name of site.
            variable_map (optional): whether to use the visualisation or pfp
            variable map. Defaults to 'pfp'.
            concat_files (optional): whether to concatenate backups.
            Defaults to True.

        Returns:
            None.

        """

        # Set site and instance of metadata manager
        self.site = site
        self.md_mngr = AugmentedMetaDataManager(
            site=site, variable_map=variable_map
            )

        # Merge the raw data
        merge_dict = {
            file: self.md_mngr.translate_variables_by_table(table=table)
            for table, file in self.md_mngr.map_tables_to_files(abs_path=True).items()
            }
        rslt = merge_all(files=merge_dict, concat_files=concat_files)
        self.data = rslt['data']
        self.headers = rslt['headers']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data(
            self, convert_units: bool=True, calculate_missing: bool=True,
            apply_limits: bool=True,
            ) -> pd.DataFrame:
        """
        Return a COPY of the raw data with QC applied (convert units,
        calculate requisite missing variables and apply rangelimits).

        Args:
            convert_units (optional): whether to convert units. Defaults to True.
            calculate_missing (optional): calculate missing variables. Defaults to True.
            apply_limits (optional): apply range limits. Defaults to True.


        Returns:
            output_data: the amended data.

        """

        output_data = self.data.copy()

        # Convert from site-based to standard units (if requested and if different)
        if convert_units:
            self._convert_units(output_data)

        # Calculate missing variables
        if calculate_missing:
            self._calculate_missing(output_data)

        # Apply range limits (if requested)
        if apply_limits:
            self._apply_limits(df=output_data)

        return output_data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _convert_units(self, df: pd.DataFrame) -> None:
        """
        Apply unit conversions.

        Args:
            df: data for unit conversion.

        Returns:
            None.

        """

        for variable in self.md_mngr.list_variables_for_conversion():

            attrs = self.md_mngr.get_variable_attributes(variable)
            func = ccf.convert_variable(variable=variable)
            df[variable] = func(df[variable], from_units=attrs['units'])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_missing(self, df: pd.DataFrame) -> None:
        """
        Calculate requisite missing quantities from existing data.

        Args:
            df: the data from which to source input variables.

        Returns:
            None.

        """

        for quantity in self.md_mngr.missing_variables.index:
            rslt = ccf.get_function(variable=quantity, with_params=True)
            args_dict = {
                parameter: self.data[parameter] for parameter in
                rslt[1]
                }
            df[quantity] = rslt[0](**args_dict)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _apply_limits(self, df: pd.DataFrame) -> None:
        """
        Apply range limits.

        Args:
            df: data to filter.

        Returns:
            None.

        """

        variables = pd.concat(
            [self.md_mngr.site_variables, self.md_mngr.missing_variables]
            )
        for variable in variables.index:
            attrs = variables.loc[variable]
            df[variable] = ccf.filter_range(
                series=df[variable],
                max_val=attrs.plausible_max,
                min_val=attrs.plausible_min
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_headers(
            self, convert_units: bool=True, include_missing: bool=True
            ) -> pd.DataFrame:
        """
        Return a COPY of the raw headers with converted units (if requested).

        Args:
            convert_units (optional): whether to return converted units.
            Defaults to True.
            include_missing (optional): add missing variables to header.
            Defaults to True.

        Returns:
            headers.

        """

        output_headers = self.headers.copy()
        if convert_units:
            output_headers['units'] = (
                self.md_mngr.site_variables.standard_units
                .reindex(output_headers.index)
                )
        if include_missing:
            try:
                append_headers = (
                    self.md_mngr.missing_variables[['standard_units']]
                    .rename_axis('variable')
                    .rename({'standard_units': 'units'}, axis=1)
                    )
                append_headers['sampling'] = ''
                output_headers = pd.concat([output_headers, append_headers])
            except KeyError:
                pass
        return output_headers
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_file(self, path_to_file: pathlib.Path | str=None) -> None:
        """
        Write data to file.

        Args:
            path_to_file (optional): output file path. If None, slow data path
            and default name used. Defaults to None.

        Returns:
            None.

        """

        if not path_to_file:
            path_to_file = (
                self.md_mngr.data_path / f'{self.site}_merged.dat'
                )
        headers = self.get_headers()
        data = self.get_data()
        io.write_data_to_file(
            headers=io.reformat_headers(headers=headers, output_format='TOA5'),
            data=io.reformat_data(data=data, output_format='TOA5'),
            abs_file_path=path_to_file,
            output_format='TOA5'
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class AugmentedMetaDataManager(mh.MetaDataManager):
    """Adds missing variable table to standard MetaDataManager class"""

    #--------------------------------------------------------------------------
    def __init__(self, site, variable_map='pfp'):

        super().__init__(site, variable_map)

        # Get missing variables and create table
        requisite_variables = (
            cm.get_global_configs(which='requisite_variables')[variable_map]
            )
        missing_variables = [
            variable for variable in requisite_variables if not variable in
            self.site_variables.quantity.unique()
            ]
        self.missing_variables = pd.DataFrame()
        if missing_variables:
            self.missing_variables = (
                self.standard_variables.loc[missing_variables]
                )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

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
        concat_files (optional): concat backup files to current.

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