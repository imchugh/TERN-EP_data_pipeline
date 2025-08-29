#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  7 16:02:37 2025

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import pandas as pd
import pathlib
import yaml

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

var_attrs = [
    'instrument', 'statistic_type', 'units', 'height', 'name', 'logger',
    'table'
    ]
optional_var_attrs = ['long_name', 'diag_type']

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN CLASSES ###
###############################################################################

#------------------------------------------------------------------------------
class ConfigsEditor():

    #--------------------------------------------------------------------------
    def __init__(self, data: pd.DataFrame, input_file: str | pathlib.Path) -> None:

        self.input_file = pathlib.Path(input_file)

        # Remove any variables to be ignored
        if 'ignore' in data.columns:
            data = data[data.ignore == False]

        # Keep optional columns if they appear
        use_cols = var_attrs.copy()
        for var in optional_var_attrs:
            if var in data.columns:
                use_cols += [var]

        # Assign data as attr
        self.data = data[use_cols]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_variables(self) -> list:

        return self.data.index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_fields(self) -> list:

        return self.data.columns.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attrs(self, variable: str) -> pd.Series | str:

        return self.data.loc[variable]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attr(
            self, field: str, variable: str=None
            ) -> pd.Series | str:

        if not variable:
            return self.data.loc[:, field]
        return self.data.loc[variable, field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def set_variable_attr(
            self, field: str, variable: str, value: str
            ) -> pd.Series | str:

        self.data.loc[variable, field] = str(value)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def add_variable(self, var_name: str, var_dict: dict=None) -> None:

        # Don't allow variable overwrites / duplicates
        if var_name in self.data.index:
            raise IndexError(f'Variable {var_name} already in index!')
        if var_dict is None:
            var_dict = {}

        # Enforce type as dict
        if not isinstance(var_dict, dict):
            raise TypeError('`var_dict` kwarg must be a dictionary!')
        attrs_dict = {attr: '' for attr in var_attrs}

        # Ignore anything in the passed dict that uses non-standard keys
        for var in attrs_dict:
            try:
                attrs_dict[var] = str(var_dict[var])
            except KeyError:
                continue

        # Write the new variable to the dataframe
        self.data = pd.concat(
            [
                self.data,
                (
                    pd.DataFrame.from_dict(
                        attrs_dict, orient='index', columns=[var_name]
                        )
                    .T
                    )
                ]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def remove_variable(self, var_name: str) -> None:

        self.data = self.data.drop(var_name)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def rename_variable(self, current_name, new_name):

        self.data = self.data.rename({current_name: new_name})
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_file(self, output_file):

        # Don't allow overwrites
        output_file = pathlib.Path(output_file)
        if output_file == self.input_file:
            raise FileExistsError(
                'Cannot overwrite existing configuration file'
                )

        # If writing to yml, first create a dict where empty fields are removed
        if output_file.suffix == '.yml':
            data = {}
            for var in self.data.index:
                s = self.data.loc[var]
                data[var] = (s[~pd.isnull(s)].to_dict())
            _write_yml(file_path=output_file, data=data)

        # Or if xlsx, send the data as is
        if output_file.suffix == '.xlsx':
            _write_xl(file_path=output_file, data=self.data)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class ConfigsGetter():

    @staticmethod
    def from_file(file_path):
        file_path = pathlib.Path(file_path)
        if file_path.suffix == '.yml':
            return _load_yml(file_path=file_path)
        if file_path.suffix == '.xlsx':
            return _load_xl(file_path=file_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _load_yml(file_path):

    with open(file_path) as f:
        data = pd.DataFrame(yaml.safe_load(stream=f)).T
    return ConfigsEditor(data=data, input_file=file_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _load_xl(file_path):

    data = pd.read_excel(file_path).set_index(keys='pfp_name')
    return ConfigsEditor(data=data, input_file=file_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_yml(file_path, data):

    with open(file=file_path, mode='w', encoding='utf-8') as f:
        yaml.dump(data=data, stream=f, sort_keys=False)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_xl(file_path, data):

    return data.to_excel(file_path, sheet_name='variables')
#------------------------------------------------------------------------------

###############################################################################
### END CLASSES ###
###############################################################################


#------------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_to_yml(data: dict, file: pathlib.Path | str) -> None:
    """
    Write data dictionary input data to file.

    Args:
        data (TYPE): DESCRIPTION.
        file (TYPE): DESCRIPTION.

    Returns:
        None.

    """

    with open(file=file, mode='w', encoding='utf-8') as f:
        yaml.dump(data=data, stream=f, sort_keys=False)
#------------------------------------------------------------------------------
