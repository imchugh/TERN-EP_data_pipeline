#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  1 15:01:13 2025

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

from configobj import ConfigObj
import pandas as pd
import pathlib
import yaml

#------------------------------------------------------------------------------

from managers import paths

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

reference_vars = {
    'turbflux': 'Fco2',
    'radflux': 'Fsd',
    'logger': 'Tpanel',
    'TandRH': 'Ta',
    'rain': 'Precip'
    }

system_suffixes = {
    'EasyFlux': 'EF',
    'TERNflux': 'DL',
    'EddyPro': 'EP'
    }

suffix_vars = ['Fco2', 'Fe', 'Fh', 'Fm']

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN CLASSES ###
###############################################################################

#------------------------------------------------------------------------------
class SiteL1ConfigGenerator():

    #--------------------------------------------------------------------------
    def __init__(self, site: str) -> None:

        self.site = site
        dfs = read_site_config_xl(site=site)
        self.system_configs = dfs['configs']['value'].to_dict()
        self.flux_suffix = (
            system_suffixes[self.system_configs['system_type'].split('_')[0]]
            )
        self.variable_configs = dfs['variables'].T.to_dict()
        self.template_configs = (
            read_config_template_xl(
                template_name=self.system_configs['system_type']
                )
            .T
            .to_dict()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compile_configs(self):

        template = self.template_configs.copy()
        template.update(self.variable_configs)

        # Create and format the dataframe
        df = pd.DataFrame(data=template, dtype='O').T
        df.index.name = 'pfp_name'
        df['ignore'] = df['ignore'].astype(bool)
        df.loc[self.variable_configs.keys(), 'ignore'] = False
        df = df[~df.ignore]
        df = df.drop('ignore', axis=1)

        # Amend heights
        for key in reference_vars:
            inst_name = df.loc[reference_vars[key], 'instrument']
            height = self.system_configs[f'{key}_height']
            df.loc[df['instrument'] == inst_name, 'height'] = height

        # Attach flux suffixes
        rslt = {}
        for var in df.index:
            for suffix_var in suffix_vars:
                if var.startswith(suffix_var):
                    rslt[var] = _meld_names(name=var, suffix=self.flux_suffix)
        df = df.rename(rslt)

        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_compiled_configs_to_yml(self):

        # Create dicts
        df = self.compile_configs()
        data = {}
        for var in df.index:
            s = df.loc[var]
            data[var] = (s[~pd.isnull(s)].to_dict())

        # Set output_path
        file = paths.get_local_stream_path(
            resource='configs_new',
            stream='site_yml',
            file_name=f'{self.site}_configs.yml'
            )

        # Dump to yml
        _write_to_yml(data=data, file=file)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _meld_names(name, suffix):

    elems = name.split('_')
    out_name = '_'.join([elems[0], suffix])
    if len(elems) == 1:
        return out_name
    if len(elems) == 2:
        return '_'.join([out_name, elems[-1]])
    raise RuntimeError('Too many elements in name (name)!')
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

        df = (
            pd.concat(
                [
                    pd.DataFrame(
                        [self.config['Variables'][key]['Attr'] for key in
                         self.config['Variables'].keys()]
                        ),
                    pd.DataFrame(
                        [self.config['Variables'][key]['xl'] for key in
                         self.config['Variables'].keys()]
                        )
                    ],
                axis=1
                )
            .set_index(key for key in self.config['Variables'].keys())
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

#------------------------------------------------------------------------------

###############################################################################
### END CLASSES ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def read_config_template_xl(template_name: str) -> pd.DataFrame:

    return (
        _read_config_xl(
            path=paths.get_local_stream_path(
                resource='configs_new', stream='template_xl'
                ),
            sheet=template_name
            )
        .set_index(keys='pfp_name')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def read_site_config_xl(site: str):

    dfs = _read_config_xl(
        path=paths.get_local_stream_path(
            resource='configs_new',
            stream='site_xl',
            file_name=f'{site}_configs.xlsx',
            check_exists=True
            ),
        sheet=None
        )
    dfs['configs'] = dfs['configs'].set_index(keys='key')
    dfs['variables'] = dfs['variables'].set_index(keys='pfp_name')
    return dfs
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _read_config_xl(path, sheet):

    return (
        pd.read_excel(
            io=path,
            sheet_name=sheet,
            dtype='O',
            )
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def read_config_template_yml(template_name: str) -> pd.DataFrame:

    # Get path to template xl file
    xlsx_template_path = (
        paths.get_local_stream_path(
            resource='configs_new', stream='template_yml'
            )
        / f'{template_name}.yml'
        )

    # Blah
    return paths.get_other_config_file(file_path=xlsx_template_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def read_site_config_yml(site: str):

    yml_path = paths.get_local_stream_path(
        resource='configs_new',
        stream='site_yml',
        file_name=f'{site}_configs.yml',
        check_exists=True
        )
    return paths.get_other_config_file(file_path=yml_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_xl_template_variables_to_yml(template_name: str) -> None:
    """


    Args:
        template_name (str): DESCRIPTION.

    Returns:
        None: DESCRIPTION.

    """

    # Get dataframe from xl template
    df = read_config_template_xl(template_name=template_name)

    # Retype ignore variable as boolean
    df['ignore'] = df['ignore'].astype(bool)

    # Drop empty variables and generate variable dicts
    data = {}
    for var in df.index:
        s = df.loc[var]
        data[var] = (s[~pd.isnull(s)].to_dict())

    output_file = (
        paths.get_local_stream_path(
            resource='configs_new', stream='template_yml'
            )
        / f'{template_name}.yml'
        )

    # ... and output
    _write_to_yml(data=data, file=output_file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_xl_site_variables_to_yml(site: str) -> None:
    """


    Args:
        site (str): DESCRIPTION.

    Returns:
        None: DESCRIPTION.

    """

    # Get path to site xl file
    xlsx_site_path = paths.get_local_stream_path(
        resource='configs_new',
        stream='site_xl',
        file_name=f'{site}_configs.xlsx',
        check_exists=True
        )

    # Get variable map from excel and format
    dfs = pd.read_excel(io=xlsx_site_path, sheet_name=None)

    # Get the base and variable configs from separate sheets
    base_configs = dfs['configs'].set_index(keys='key')
    variable_configs = (
        dfs['variables']
        .astype('O')
        .set_index(keys='pfp_name')
        )

    # Collate to dict for output to yml
    data = {
        'base_configs': base_configs.value.to_dict(),
        'variable_configs': variable_configs.T.to_dict()
        }

    # Get output path
    yml_site_path = paths.get_local_stream_path(
        resource='configs_new',
        stream='site_yml',
        file_name=f'{site}_configs.yml',
        )

    # ... and output
    _write_to_yml(data=data, file=yml_site_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def compile_site_config_yml(site: str) -> None:
    """


    Args:
        site (str): DESCRIPTION.

    Returns:
        None: DESCRIPTION.

    """

    # Get the site base yml path
    site_base_yml_path = paths.get_local_stream_path(
        resource='configs_new',
        stream='site_yml',
        )

    # Get the site input and output yml paths
    site_input_yml_path = site_base_yml_path / f'{site}_configs.yml'
    site_output_yml_path = site_base_yml_path / f'{site}_pfp_variables.yml'

    # Get the site yml containing base configurations and custom variables
    site_configs = paths.get_other_config_file(file_path=site_input_yml_path)
    site_variable_configs = site_configs.pop('variable_configs')
    site_base_configs = site_configs.pop('base_configs')

    # Get the template yml path
    template_yml_path = paths.get_local_stream_path(
        resource='configs_new',
        stream='template_yml',
        file_name=f'{site_base_configs["system_type"]}.yml',
        check_exists=True
        )

    # Get the template yml containing standard variables
    template_variable_configs = paths.get_other_config_file(
        file_path=template_yml_path
        )

    # Combine the template and custom variables
    template_variable_configs.update(site_variable_configs)

    # Create and format the dataframe
    df = (
        pd.DataFrame(data=template_variable_configs, dtype='O')
        .T
        )
    df.index.name = 'pfp_name'
    df['ignore'] = df['ignore'].astype(bool)
    df.loc[site_variable_configs.keys(), 'ignore'] = False
    df = df[~df.ignore]
    df = df.drop('ignore', axis=1)

    # Amend heights
    for key in reference_vars:
        inst_name = df.loc[reference_vars[key], 'instrument']
        height = site_base_configs[f'{key}_height']
        df.loc[df['instrument'] == inst_name, 'height'] = height

    # Create dicts
    data = {}
    for var in df.index:
        s = df.loc[var]
        data[var] = (s[~pd.isnull(s)].to_dict())

    # ... and output
    _write_to_yml(data=data, file=site_output_yml_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_to_yml(data: dict, file: pathlib.Path | str) -> None:
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

###############################################################################
### END FUNCTIONS ###
###############################################################################

# def _convert_input_excel_to_yml_output(input_file, output_file):

#     output_vars = metadata.REQUISITE_FIELDS.copy()

#     df = (
#         pd.read_excel(io=input_file, sheet_name='Variable_attrs', dtype='O')
#         .set_index(keys='pfp_name')
#         .fillna('')
#         )

#     # Drop variables we wish to ignore in the excel file
#     if 'ignore' in df.columns:
#         df = df[~df.ignore.astype(bool)]
#         df = df.drop('ignore', axis=1)

#     # If long_name is in the columns, add to list of output variables
#     if 'long_name' in df.columns:
#         output_vars.insert(4, 'long_name')

#     # If diag_type is in the columns, add to list of output variables
#     if 'diag_type' in df.columns:
#         output_vars.append('diag_type')

#     # Subset the dataframe
#     df = df[output_vars]

#     rslt = {}
#     for var in df.index:
#         s = df.loc[var]
#         rslt[var] = (s[s != ''].to_dict())

#     with open(file=output_file, mode='w', encoding='utf-8') as f:
#         yaml.dump(data=rslt, stream=f, sort_keys=False)

# def get_site_configs():

#     site_info = pd.read_excel(
#         io=xlsx_site_path,
#         sheet_name=None
#         )

#     return (
#         site_info['configs'].set_index(keys='key'),
#         site_info['variables'].set_index(keys='pfp_name')
#         )

# def build_site_template():

#     site_configs, site_variables = get_site_configs()

#     template_df = pd.read_excel(
#         io=xlsx_template_path,
#         sheet_name=site_configs.loc['system_type', 'value'],
#         index_col='pfp_name'
#         )

#     for key in reference_vars:
#         inst_name = template_df.loc[reference_vars[key], 'instrument']
#         height = str(site_configs.loc[f'{key}_height', 'value']) + 'm'
#         template_df.loc[
#             template_df['instrument'] == inst_name, 'height'
#             ] = height

#     # breakpoint()

#     return template_df





# df = build_site_template()

