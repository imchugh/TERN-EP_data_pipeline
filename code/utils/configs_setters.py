# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 12:32:42 2024

This file contains code to generate the yaml configuration files that is
required to generate L1 netcdf files. At PRESENT, the files just read info from
an excel file, but ULTIMATELY the files will hopefully be generated from a
web-based UI where the PI can enter the requisite info.

@author: jcutern-imchugh
"""

import ast
import pandas as pd
import pathlib
import yaml
from configobj import ConfigObj

import io
from paths import paths_manager as pm
# from .site_details import SiteDetails

GLOBAL_ATTRS = [
    'Conventions', 'acknowledgement', 'altitude',  'canopy_height', 'comment',
    'contact', 'data_link', 'featureType', 'fluxnet_id', 'history',
    'institution', 'latitude', 'license', 'license_name', 'longitude',
    'metadata_link', 'ozflux_link', 'publisher_name', 'references',
    'site_name', 'site_pi', 'soil', 'source', 'time_step', 'time_zone',
    'title', 'tower_height', 'vegetation'
    ]

GENERIC_GLOBAL_ATTRS = {
    'Conventions': 'CF-1.8',
    'acknowledgement': 'This work used eddy covariance data collected by '
    'the TERN Ecosystem \nProcesses facility. Ecosystem Processes would '
    'like to acknowledge the financial support of the \nAustralian Federal '
    'Government via the National Collaborative Research Infrastructure '
    'Scheme \nand the Education Investment Fund.',
    'comment': 'CF metadataOzFlux standard variable names',
    'featureType': 'timeSeries',
    'license': 'https://creativecommons.org/licenses/by/4.0/',
    'license_name': 'CC BY 4.0',
    'metadata_link': 'http://www.ozflux.org.au/monitoringsites/<site>/index.html',
    'ozflux_link': 'http://ozflux.org.au/',
    'publisher_name': 'TERN Ecosystem ProcessesOzFlux',
    'processing_level': 'L1',
    'data_link': 'http://data.ozflux.org.au/',
    'site_name': '<site>'
    }

ALIAS_DICT = {'elevation': 'altitude'}

DEVICES = ['modem', 'logger', 'camera']

# Details = SiteDetails()

###############################################################################
### BEGIN MULTI-SITE CONFIGURATION GENERATOR SECTION ###
###############################################################################

# class HardwareConfigsGenerator():
#     """Class to read and interrogate hardware configuration from excel file,
#     and write operational configuration files - mostly used if config input
#     data changes!
#     """

#     def __init__(self):

#         self.modem_table = _read_excel_fields(sheet_name='Modems')
#         self.modem_fields = self.modem_table.columns.tolist()
#         self.sites = self.modem_table.index.tolist()
#         self.logger_table = (
#             pd.read_excel(
#                 io=paths_mngr.get_local_stream_path(
#                     resource='network', stream='xl_connections_manager'
#                     ),
#                 sheet_name='Loggers',
#                 dtype={
#                     field: 'Int64' for field in
#                     ['serial_num', 'tcp_port', 'pakbus_addr']
#                     }
#                 )
#             .set_index(keys='Site')
#             .sort_index()
#             )
#         self.logger_table.tables = (
#             self.logger_table.tables
#             .fillna('')
#             .apply(lambda x: x.split(','))
#             )
#         self.logger_fields = self.logger_table.columns.tolist()

#     def get_site_logger_list(self, site: str) -> list:
#         """
#         Get list of loggers for a given site.

#         Args:
#             site: name of site.

#         Returns:
#             List of loggers.

#         """

#         return self.logger_table.loc[[site], 'logger'].tolist()

#     def get_site_logger_details(
#             self, site: str, logger: str=None, field: str=None
#             ) -> pd.DataFrame | pd.Series | str:
#         """
#         Get details of loggers for a given site.

#         Args:
#             site: the site.
#             logger: logger name for which to return details. Defaults to None.
#             field: field to return. Defaults to None.

#         Returns:
#             Logger details. If optional kwargs are not specified, returns a
#             string. If logger is specified, return a series.
#             If field is specified, return a str.

#         """

#         sub_df = self.logger_table.loc[[site]].set_index(keys='logger')
#         if not logger is None:
#             sub_df = sub_df.loc[logger]
#         if not field is None:
#             sub_df = sub_df[[field]]
#         if not field:
#             return sub_df
#         return sub_df[field]

#     def get_site_modem_details(
#             self, site: str, field: str=None
#             ) -> pd.Series | str:
#         """
#         Get details of modem for a given site.

#         Args:
#             site: name of site.
#             field: field to return. Defaults to None.

#         Returns:
#             Details of modem.

#         """

#         if field is None:
#             return self.modem_table.loc[site]
#         return self.modem_table.loc[site, field]

#     def get_routable_sites(self) -> list:
#         """
#         Get list of sites that (should) have working ovpn connections.

#         Returns:
#             The sites.

#         """

#         return (
#             self.modem_table[self.modem_table.routable!=0]
#             .index
#             .unique()
#             .tolist()
#             )

#     #--------------------------------------------------------------------------
#     def build_configs_dict(self, site: str) -> dict:
#         """
#         Build configuration dictionary.

#         Args:
#             site: name of site.

#         Returns:
#             The dictionary.

#         """

#         return {
#             'modem': self.get_site_modem_details(site=site).to_dict(),
#             'loggers': {
#                 logger:
#                     self.get_site_logger_details(
#                         site=site, logger=logger
#                          ).to_dict()
#                 for logger in self.get_site_logger_list(site=site)
#                 }
#             }
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def dump_config_to_file(self, site: str, write_fmt='yaml'):
#         """


#         Args:
#             site (str): DESCRIPTION.
#             out_fmt (TYPE, optional): DESCRIPTION. Defaults to 'yml'.

#         Raises:
#             NotImplementedError: DESCRIPTION.

#         Returns:
#             None.

#         """

#         fmt_dict = {'yaml': 'yml', 'json': 'json'}
#         out_file = (
#             PathsManager.get_local_resource_path(
#                 resource='config_files', subdirs=['Hardware']
#                 ) /
#             f'{site}_hardware.{fmt_dict[write_fmt]}'
#             )
#         rslt = self.build_configs_dict(site=site)
#         with open(file=out_file, mode='w', encoding='utf-8') as f:
#             if write_fmt == 'yaml':
#                 yaml.dump(data=rslt, stream=f, sort_keys=False)
#             elif write_fmt == 'json':
#                 json.dump(rslt, f, indent=4)
#             else:
#                 raise NotImplementedError('Unrecognised format!')
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def table_to_file_map(
#             self, site: str, logger:str, raise_if_no_file: bool=True,
#             paths_as_str: bool=False
#             ) -> dict:
#         """
#         Tie table names to local file locations.

#         Args:
#             site: name of site.
#             logger: logger: logger name for which to provide mapping.
#             raise_if_no_file: raise exception if the file does not exist. Defaults to True.
#             paths_as_str: output paths as strings (instead of pathlib). Defaults to False.

#         Raises:
#             FileNotFoundError: DESCRIPTION.

#         Returns:
#             Dictionary mapping table (key) to absolute file path (value).

#         """

#         details = self.get_site_logger_details(site=site, logger=logger)
#         dir_path = PathsManager.get_local_data_path(
#             site=site, data_stream='flux_slow'
#             )
#         rslt = {
#             table: dir_path / f'{site}_{logger}_{table}.dat'
#             for table in details['tables'].split(',')
#             }
#         if raise_if_no_file:
#             for key, val in rslt.items():
#                 if not val.exists():
#                     raise FileNotFoundError(
#                         f'No file named {val} exists for table {key}!'
#                         )
#         if paths_as_str:
#             return {key: str(value) for key, value in rslt.items()}
#         return rslt
#     #--------------------------------------------------------------------------

# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def _read_excel_fields(sheet_name, dtype={}):

#     return (
#         pd.read_excel(
#             io=paths_mngr.get_local_stream_path(
#                 resource='network', stream='xl_connections_manager'
#                 ),
#             sheet_name=sheet_name,
#             dtype=dtype
#             )
#         .set_index(keys='Site')
#         .sort_index()
#         )
# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SiteConfigsGenerator():
    """Get configurations from site-based configs spreadsheet (and optionally
    write to yml file)"""

    #--------------------------------------------------------------------------
    def __init__(self, site: str):
        """
        Set the site attribute and read the xl configuration file.

        Args:
            site: name of site.

        Returns:
            None.

        """

        self.site=site
        self._xl = pd.ExcelFile(f'/store/Config_files/Site_based/xl_configs/{site}.xlsx'
            # paths_mngr.get_local_stream_path(
                # resource='configs',
                # stream='site_xl',
                # file_name=f'{self.site}.xlsx',
                # check_exists=True
                # )
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_sheet_names(self) -> list:
        """
        Get the names of the sheets in the xl workbook.

        Returns:
            list of sheet names.

        """
        return self._xl.sheet_names
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_all_hardware_configs(self) -> dict:
        """
        Get the configuration information for modem, logger and camera.

        Returns:
            the info.

        """

        return {
            which: self.get_hardware_configs(which=which)
            for which in DEVICES
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_hardware_configs(
            self, device: str=None, write: bool=False
            ) -> dict:
        """
        Get the configuration information for a specific device (modem, logger
        or camera).

        Args:
            which (optional): device for which to grab info. Defaults to None.
            write (optional): write to disk. Defaults to False.

        Returns:
            the info.

        """

        if device is None:
            rslt = {
                this_device: self._get_configs(sheet_name=this_device)
                for this_device in DEVICES
                }
            device = 'hardware'
        else:
            rslt = self._get_configs(sheet_name=device)
        if not write:
            return rslt
        self._write_to_yml(
            rslt=rslt,
            subdir=['Hardware'],
            file_name_elem=device
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_configs(self, sheet_name: str) -> dict:
        """
        Get the device dataframe from the xl parser object.

        Args:
            device: device type name (see DEVICES).

        Returns:
            configurations.

        """

        return (
            self._xl.parse(sheet_name)
            .set_index('name')
            .pipe(self._listify_old, 'tables')
            .fillna('')
            .squeeze()
            .T
            .to_dict()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_configs(self, which: str, write: bool=False) -> dict:
        """
        Get the variable configuration information.

        Args:
            which: the variable type (`L1` or `Vis`).
            write (optional): write to disk. Defaults to False.

        Returns:
            the info.

        """

        sheet_name = f'{which}_variables'
        rslt = (
            self._xl.parse(sheet_name=sheet_name)
            .set_index('variable')
            .fillna('')
            .astype(str)
            .T
            .to_dict()
            )
        if not write:
            return rslt
        self._write_to_yml(
            rslt=rslt,
            subdir=['Variables'],
            file_name_elem=which
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _listify_old(
            self, data: pd.core.frame.DataFrame, series_name
            ) -> pd.core.frame.DataFrame:
        """
        Convert comma-separated string to list.

        Args:
            data: the data containing the comma-separated table string.
            series_name: the name of the series for which elements will be listified.

        Returns:
            data: the data with listified series.

        """

        try:
            data[series_name] = data[series_name].apply(lambda x: x.split(','))
            return data
        except KeyError:
            return data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _write_to_yml(self, rslt: dict, subdir: list, file_name_elem: str):
        """
        Write the passed dictionary to yml.

        Args:
            rslt: the data to write.
            subdir: the subdirectory to append to the base directory.
            file_name_elem: the string element to insert into the file name.

        Returns:
            None.

        """

        out_path=pm.get_local_stream_path(
            resource='configs', stream='variables_vis', site=self.site
            )
        with open(file=out_path, mode='w', encoding='utf-8') as f:
            yaml.dump(data=rslt, stream=f, sort_keys=False)
    #--------------------------------------------------------------------------

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
def pfp_std_names_to_yml():
    """
    Write yml configuration file of pfp naming conventions.

    Returns:
        None.

    """

    file_path = pm.get_local_resource_path(
        resource='configs', subdirs=['Globals']
        )
    data = (
        pd.read_excel(
            io=file_path / 'pfp_std_names.xlsx',
            sheet_name='names',
            )
        .pipe(_selective_strip)
        .fillna('')
        .set_index(keys='pfp_name')
        .T
        .to_dict()
        )
    with open(file=file_path / 'pfp_std_names.yml', mode='w', encoding='utf-8') as f:
        yaml.dump(data=data, stream=f, sort_keys=False)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _selective_strip(df):

    for this_col in df.select_dtypes(include='object').columns:
        df[this_col] = df[this_col].astype(str).str.strip()
    return df
#------------------------------------------------------------------------------

def PFPL1XlToYml(
        file_name_xl: pathlib.Path | str,
        file_name_yml: pathlib.Path | str=None
        ) -> None:
    """
    Read the excel file containing the variable configurations and generate
    a yaml file.

    Args:
        site: name of site.

    Returns:
        None.

    """

    output_vars = [
        'height', 'instrument', 'statistic_type', 'units', 'name', 'logger',
        'table'
        ]

    df = (
        pd.read_excel(io=file_name_xl, sheet_name='Variable_attrs')
        .set_index(keys='pfp_name')
        .fillna('')
        )

    if 'ignore' in df.columns:
        df = df[~df.ignore.astype(bool)]
    if 'long_name' in df.columns:
        output_vars.insert(4, 'long_name')
    df = df[output_vars]

    rslt = df.T.to_dict()
    for var, attrs in rslt.items():
        try:
            if len(attrs['long_name']) == 0:
                attrs.pop('long_name')
        except KeyError:
            pass

    if file_name_yml is None:
        file_name_xl = pathlib.Path(file_name_xl)
        file_name_yml = file_name_xl.parent / f'{file_name_xl.stem}.yml'

    with open(file=file_name_yml, mode='w', encoding='utf-8') as f:
        yaml.dump(data=rslt, stream=f, sort_keys=False)



def PFPL1XlToYml_old(site: str):
    """
    Read the excel file containing the variable configurations and generate
    a yaml file.

    Args:
        site: name of site.

    Returns:
        None.

    """

    variable_path = pm.get_local_resource_path(
        resource='config_files', subdirs=['Variables']
        )

    df = (
        io.read_excel(
            file=variable_path / f'{site}_variables.xlsx',
            sheet_name='Variable_attrs'
            )
        .set_index(keys='Variable')
        .drop(['long_name', 'standard_name'], axis=1)
        )

    breakpoint()
    with open(
            file=variable_path / f'{site}_variables.yml', mode='w',
            encoding='utf-8'
            ) as f:
        yaml.dump(data=df.T.to_dict(), stream=f, sort_keys=False)

# def get_L1_site_global_attrs(site: str) -> dict:
#     """


#     Args:
#         site (TYPE): DESCRIPTION.

#     Returns:
#         TYPE: DESCRIPTION.

#     """

#     subset = [
#         'fluxnet_id', 'latitude', 'longitude', 'elevation', 'time_step',
#         'time_zone'
#         ]
#     return (
#         Details.get_single_site_details(site=site)
#         [subset]
#         .rename(ALIAS_DICT)
#         .to_dict()
#         )

def write_L1_generic_global_attrs():
    """
    Write the generic global attributes dictionary to a file in json format.

    Returns:
        None.

    """

    with open(file=_get_generic_globals_file(), mode='w', encoding='utf-8') as f:
        yaml.dump(data=GENERIC_GLOBAL_ATTRS, stream=f, sort_keys=False)

def _get_generic_globals_file() -> str | pathlib.Path:
    """
    Return the absolute path of the global generic attributes file.

    Returns:
        The path.

    """

    return (
        pm.get_local_resource_path(
            resource='configs', subdirs=['Globals']
            ) /
        'generic_global_attrs.yml'
        )

def convert_xl_variables_to_yml(site):

    input_path = (
        pm.get_local_stream_path(resource='configs', stream='site_xl') /
        f'{site}.xlsx'
        )
    output_path = (
        pm.get_local_stream_path(
            resource='configs', stream='variables_vis', site='Whroo'
            )
        )

    data = (
        pd.read_excel(input_path)
        .set_index('variable')
        .pipe(_listify_old, 'tables')
        .fillna('')
        .squeeze()
        .T
        .to_dict()
        )

    _write_yml(file=output_path, data=data)

def _listify_old(data: pd.DataFrame, series_name) -> pd.DataFrame:
    """
    Convert comma-separated string to list.

    Args:
        data: the data containing the comma-separated table string.
        series_name: the name of the series for which elements will be listified.

    Returns:
        data: the data with listified series.

    """

    try:
        data[series_name] = data[series_name].apply(lambda x: x.split(','))
        return data
    except KeyError:
        return data

def _write_yml(file: pathlib.Path | str, data: dict) -> None:

    with open(file, mode='w', encoding='utf-8') as f:
        yaml.dump(data=data, stream=f, sort_keys=False)

def _delistify(elem: str | list) -> str:

    try:
        literal_elem = ast.literal_eval(elem)
        if isinstance(literal_elem, list):
            return ', '.join(literal_elem)
        return elem
    except (ValueError, SyntaxError):
        return elem
    raise TypeError('`elem` must be of type list or str!')

def _stringify_list(elem: str | list) -> str:

    if isinstance(elem, str):
        return elem
    elif isinstance(elem, list):
        return ','.join(elem)
    raise TypeError('`elem` must be of type list or str!')
