# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 12:32:42 2024

This file contains code to generate the yaml configuration files that is
required to generate L1 netcdf files. At PRESENT, the files just read info from
an excel file, but ULTIMATELY the files will hopefully be generated from a
web-based UI where the PI can enter the requisite info.

@author: jcutern-imchugh
"""

import json
import pandas as pd
import pathlib
import yaml
from configobj import ConfigObj

import file_io as io
from paths_manager import Paths
from sparql_site_details import site_details

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

PathsManager = Paths()
Details = site_details()

###############################################################################
### BEGIN MULTI-SITE CONFIGURATION GENERATOR SECTION ###
###############################################################################

class ConfigsGenerator():
    """Class to read and interrogate hardware configuration from excel file,
    and write operational configuration files - mostly used if config input
    data changes!
    """

    def __init__(self):

        self.modem_table = _read_excel_fields(sheet_name='Modems')
        self.modem_fields = self.modem_table.columns.tolist()
        self.sites = self.modem_table.index.tolist()
        self.logger_table = (
            pd.read_excel(
                io=PathsManager.get_local_resource_path(
                    resource='xl_connections_manager'
                    ),
                sheet_name='Loggers',
                dtype={
                    field: 'Int64' for field in
                    ['serial_num', 'tcp_port', 'pakbus_addr']
                    }
                )
            .set_index(keys='Site')
            .sort_index()
            )
        self.logger_table.tables = (
            self.logger_table.tables
            .fillna('')
            .apply(lambda x: x.split(','))
            )
        self.logger_fields = self.logger_table.columns.tolist()

    def get_site_logger_list(self, site: str) -> list:
        """
        Get list of loggers for a given site.

        Args:
            site: name of site.

        Returns:
            List of loggers.

        """

        return self.logger_table.loc[[site], 'logger'].tolist()

    def get_site_logger_details(
            self, site: str, logger: str=None, field: str=None
            ) -> pd.DataFrame | pd.Series | str:
        """
        Get details of loggers for a given site.

        Args:
            site: the site.
            logger: logger name for which to return details. Defaults to None.
            field: field to return. Defaults to None.

        Returns:
            Logger details. If optional kwargs are not specified, returns a
            string. If logger is specified, return a series.
            If field is specified, return a str.

        """

        sub_df = self.logger_table.loc[[site]].set_index(keys='logger')
        if not logger is None:
            sub_df = sub_df.loc[logger]
        if not field is None:
            sub_df = sub_df[[field]]
        if not field:
            return sub_df
        return sub_df[field]

    def get_site_modem_details(
            self, site: str, field: str=None
            ) -> pd.Series | str:
        """
        Get details of modem for a given site.

        Args:
            site: name of site.
            field: field to return. Defaults to None.

        Returns:
            Details of modem.

        """

        if field is None:
            return self.modem_table.loc[site]
        return self.modem_table.loc[site, field]

    def get_routable_sites(self) -> list:
        """
        Get list of sites that (should) have working ovpn connections.

        Returns:
            The sites.

        """

        return (
            self.modem_table[self.modem_table.routable!=0]
            .index
            .unique()
            .tolist()
            )

    #--------------------------------------------------------------------------
    def build_configs_dict(self, site: str) -> dict:
        """
        Build configuration dictionary.

        Args:
            site: name of site.

        Returns:
            The dictionary.

        """

        return {
            'modem': self.get_site_modem_details(site=site).to_dict(),
            'loggers': {
                logger:
                    self.get_site_logger_details(
                        site=site, logger=logger
                         ).to_dict()
                for logger in self.get_site_logger_list(site=site)
                }
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def dump_config_to_file(self, site: str, write_fmt='yaml'):
        """


        Args:
            site (str): DESCRIPTION.
            out_fmt (TYPE, optional): DESCRIPTION. Defaults to 'yml'.

        Raises:
            NotImplementedError: DESCRIPTION.

        Returns:
            None.

        """

        fmt_dict = {'yaml': 'yml', 'json': 'json'}
        out_file = (
            PathsManager.get_local_resource_path(
                resource='config_files', subdirs=['Hardware']
                ) /
            f'{site}_hardware.{fmt_dict[write_fmt]}'
            )
        rslt = self.build_configs_dict(site=site)
        with open(file=out_file, mode='w', encoding='utf-8') as f:
            if write_fmt == 'yaml':
                yaml.dump(data=rslt, stream=f, sort_keys=False)
            elif write_fmt == 'json':
                json.dump(rslt, f, indent=4)
            else:
                raise NotImplementedError('Unrecognised format!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def table_to_file_map(
            self, site: str, logger:str, raise_if_no_file: bool=True,
            paths_as_str: bool=False
            ) -> dict:
        """
        Tie table names to local file locations.

        Args:
            site: name of site.
            logger: logger: logger name for which to provide mapping.
            raise_if_no_file: raise exception if the file does not exist. Defaults to True.
            paths_as_str: output paths as strings (instead of pathlib). Defaults to False.

        Raises:
            FileNotFoundError: DESCRIPTION.

        Returns:
            Dictionary mapping table (key) to absolute file path (value).

        """

        details = self.get_site_logger_details(site=site, logger=logger)
        dir_path = PathsManager.get_local_data_path(
            site=site, data_stream='flux_slow'
            )
        rslt = {
            table: dir_path / f'{site}_{logger}_{table}.dat'
            for table in details['tables'].split(',')
            }
        if raise_if_no_file:
            for key, val in rslt.items():
                if not val.exists():
                    raise FileNotFoundError(
                        f'No file named {val} exists for table {key}!'
                        )
        if paths_as_str:
            return {key: str(value) for key, value in rslt.items()}
        return rslt
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _read_excel_fields(sheet_name, dtype={}):

    return (
        pd.read_excel(
            io=PathsManager.get_local_resource_path(
                resource='xl_connections_manager'
                ),
            sheet_name=sheet_name,
            dtype=dtype
            )
        .set_index(keys='Site')
        .sort_index()
        )
#------------------------------------------------------------------------------

###############################################################################
### END MULTI-SITE CONFIGURATION GENERATOR SECTION ###
###############################################################################

class PFPL1CntlToXl():
    """Class to convert L1 control files to excel workbooks (one sheet contains
    the global fields, one sheet contains the variable fields). The 'Variables'
    sheet then needs to be manually amended so that the 'sheet' field is
    replaced with information about the logger and table that record the given
    variable.
    """

    def __init__(self, filename):

        self.config=ConfigObj(filename)
        self.site = self.config['Global']['site_name']


    def get_variable_table(self) -> pd.DataFrame:
        """
        Grab all the variable attributes from the L1 control file.

        Returns:
            The variable attributes.

        """

        return (
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
            )

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

    def write_variables_to_excel(self, xl_write_path: pathlib.Path | str):
        """
        Generate an excel file containing the global and variable configs.

        Args:
            xl_write_path: path to write excel file to.

        Returns:
            None.

        """

        vars_df = self.get_variable_table()
        globals_series = self.get_globals_series()
        with pd.ExcelWriter(path=xl_write_path) as writer:
            globals_series.to_excel(writer, sheet_name='Global_attrs')
            vars_df.to_excel(writer, sheet_name='Variable_attrs')

def pfp_std_names_to_yaml():
    """
    Write yml configuration file of pfp naming conventions.

    Returns:
        None.

    """

    io_path = PathsManager.get_local_resource_path(
        resource='config_files', subdirs=['Globals']
        )
    data = (
        io.read_excel(
            file=io_path / 'std_names.xlsx',
            sheet_name='names'
            )
        .fillna('')
        .apply(lambda x: x.str.strip())
        .set_index(keys='pfp_name')
        .T
        .to_dict()
        )
    with open(file=io_path / 'std_names.yml', mode='w', encoding='utf-8') as f:
        yaml.dump(data=data, stream=f, sort_keys=False)

def PFPL1XlToYaml(site: str):
    """
    Read the excel file containing the variable configurations and generate
    a yaml file.

    Args:
        site: name of site.

    Returns:
        None.

    """

    variable_path = PathsManager.get_local_resource_path(
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

    with open(
            file=variable_path / f'{site}_variables.yml', mode='w',
            encoding='utf-8'
            ) as f:
        yaml.dump(data=df.T.to_dict(), stream=f, sort_keys=False)

def get_L1_site_global_attrs(site: str) -> dict:
    """


    Args:
        site (TYPE): DESCRIPTION.

    Returns:
        TYPE: DESCRIPTION.

    """

    subset = [
        'fluxnet_id', 'latitude', 'longitude', 'elevation', 'time_step',
        'time_zone'
        ]
    return (
        Details.get_single_site_details(site=site)
        [subset]
        .rename(ALIAS_DICT)
        .to_dict()
        )

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
        PathsManager.get_local_resource_path(
            resource='config_files', subdirs=['Globals']
            ) /
        'generic_global_attrs.yml'
        )
