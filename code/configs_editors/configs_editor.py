#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  7 16:02:37 2025

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

from configobj import ConfigObj
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
    def rename_variable_by_map(self, map_dict):

        self.data = self.data.rename(map_dict)
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

    return data.to_excel(
        file_path, sheet_name='variables', index_label='pfp_name'
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class PFPL1CntlParser():
    """Class to convert L1 control files to excel workbooks (one sheet contains
    the global fields, one sheet contains the variable fields). The 'Variables'
    sheet then needs to be manually amended so that the 'sheet' field is
    replaced with information about the logger and table that record the given
    variable.
    """

    #--------------------------------------------------------------------------
    def __init__(self, file_name):

        self.config=ConfigObj(file_name)
        self.site = self.config['Global']['site_name']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_table(self) -> pd.DataFrame:
        """
        Grab all the variable attributes from the L1 control file.

        Returns:
            The variable attributes.

        """

        parse_list = []
        for variable in self.config['Variables'].keys():
            if 'xl' in self.config['Variables'][variable]:
                parse_list.append(variable)

        df = (
            pd.concat(
                [
                    # pd.DataFrame(
                    #     [self.config['Variables'][key]['Attr'] for key in
                    #      self.config['Variables'].keys()]
                    #     ),
                    # pd.DataFrame(
                    #     [self.config['Variables'][key]['xl'] for key in
                    #      self.config['Variables'].keys()]
                    #     )
                    pd.DataFrame(
                        [self.config['Variables'][key]['Attr'] for key in
                         parse_list]
                        ),
                    pd.DataFrame(
                        [self.config['Variables'][key]['xl'] for key in
                         parse_list]
                        )
                    ],
                axis=1
                )
            # .set_index(key for key in self.config['Variables'].keys())
            .set_index(key for key in parse_list)
            .rename({'sheet': 'table'}, axis=1)
            .fillna('')
            )
        df['instrument'] = df['instrument'].apply(_stringify_list)
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_globals_series(self):
        """
        Grab all the global attributes from the L1 control file.

        Returns:
            The global attributes.

        """

        return pd.Series(
            dict(zip(
                self.config['Global'].keys(),
                [''.join(x) for x in self.config['Global'].values()]
                ))
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_variables_to_excel(
            self, xl_write_path: pathlib.Path | str, dump_req_only=True
            ) -> None:
        """
        Generate an excel file containing the global and variable configs.

        Args:
            xl_write_path: path to write excel file to.

        Returns:
            None.

        """

        with pd.ExcelWriter(path=xl_write_path) as writer:
            self.get_globals_series().to_excel(
                writer, sheet_name='Global_attrs', header=False
                )
            self.get_variable_table().to_excel(
                writer, sheet_name='Variable_attrs', index_label='pfp_name'
                )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _stringify_list(elem: str | list) -> str:

    if isinstance(elem, str):
        return elem
    elif isinstance(elem, list):
        return ','.join(elem)
    raise TypeError('`elem` must be of type list or str!')
#------------------------------------------------------------------------------

###############################################################################
### END CLASSES ###
###############################################################################

def convert_cm_to_m(var_name):

    if not 'cm' in var_name:
        raise TypeError('Only pass variables with a depth identifier in cm!')
    elems = var_name.split('_')
    quant = elems[0]
    loc = elems[1]
    other = elems[2:]
    if not 'cm' in loc:
        raise TypeError('Variable must have location identifiers in second slot!')
    loc_elems = elems[1].split('cm')
    new_loc = str(int(loc_elems[0]) / 100).rstrip('0') + 'm' + loc_elems[1]

    return '_'.join([quant, new_loc] + other)

def convert_height_attr(old_attr):

    elems = old_attr.split('to')
    if len(elems) == 1:
        return elems[0].replace(' ','')
    for i, elem in enumerate(elems):
        elems[i] = elem.replace(' ', '')
    if not 'm' in elems[0]:
        elems[0] += 'm'
    return elems[0] + ' to ' + elems[1]


# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def write_to_yml(data: dict, file: pathlib.Path | str) -> None:
#     """
#     Write data dictionary input data to file.

#     Args:
#         data (TYPE): DESCRIPTION.
#         file (TYPE): DESCRIPTION.

#     Returns:
#         None.

#     """

#     with open(file=file, mode='w', encoding='utf-8') as f:
#         yaml.dump(data=data, stream=f, sort_keys=False)
# #------------------------------------------------------------------------------
