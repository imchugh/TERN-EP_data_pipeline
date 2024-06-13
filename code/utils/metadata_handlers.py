# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 15:29:34 2024

@author: jcutern-imchugh
"""

import numpy as np
import pandas as pd
import pathlib

import utils.configs_manager as cm
import data_handling.file_io as io
from sparql_site_details import site_details


paths = cm.PathsManager()
VALID_DEVICES = ['SONIC', 'IRGA', 'RAD']
VALID_LOC_UNITS = ['cm', 'm']
VALID_SUFFIXES = {
    'Av': 'average', 'Sd': 'standard deviation', 'Vr': 'variance'
    }

###############################################################################
### BEGIN SINGLE SITE CONFIGURATION READER SECTION ###
###############################################################################

#------------------------------------------------------------------------------
class MetaDataManager():
    """Class to read and interrogate variable data from site-specific config file"""

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def __init__(self, site: str, read_fmt: str='yaml'):
        """
        Do inits - read the yaml / json files, build lookup tables.

        Args:
            site: name of site.

        Returns:
            None.

        """

        # Set basic attrs
        self.site = site
        self.site_details = site_details().get_single_site_details(site=site)
        self.data_path = (
            paths.get_local_stream_path(
                site=site, resource='data', stream='flux_slow'
                )
            )

        # Make the variable configuration dict
        self.variable_configs = cm.get_site_configs(site=site, which='variables')
        self.variables = list(self.variable_configs.keys())

        # Make lookup tables
        self.variable_lookup_table = make_variable_lookup_table(site=site)

        # Get flux instrumewnt types
        self.irga_type = self._get_inst_type('IRGA')
        self.sonic_type = self._get_inst_type('SONIC')

        # Private inits
        self._NAME_MAP = {'site_name': 'name', 'pfp_name': 'pfp_name'}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_inst_type(self, inst):

        var_list = [x for x in self.variable_lookup_table.index if inst in x]
        inst_list = self.variable_lookup_table.loc[var_list].instrument.unique()
        if not len(inst_list) == 1:
            raise RuntimeError(
                'More than one instrument specified as instrument attribute '
                f'for {inst} device variable ({", ".join(inst_list)})'
                )
        return inst_list[0]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_attrs(self, file):

        return (
            io.get_file_info(file=self.data_path / file) |
            io.get_start_end_dates(file=self.data_path / file)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_tables(self):
        """


        Returns:
            TYPE: DESCRIPTION.

        """

        return self.variable_lookup_table.table.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_files(self):

        return self.variable_lookup_table.file.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_tables_to_files(
        self, table: str | list=None, abs_path: bool=False
        ) -> dict:

        if table is None:
            table = self.list_tables()

        s = (
            pd.Series(
                data=self.variable_lookup_table.file.tolist(),
                index=self.variable_lookup_table.table.tolist()
                )
            .drop_duplicates()
            .loc[table]
            )

        if abs_path:
            return (
                s.apply(lambda x: self.data_path / x)
                .to_dict()
                )

        return s.to_dict()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attributes(
            self, variable: str, source_field: str='pfp_name',
            return_field: str=None
            ) -> pd.Series | str:
        """
        Get the attributes for a given variable

        Args:
            variable: the variable for which to return the attribute(s).
            source_field (optional): the source field for the variable name
            (either 'pfp_name' or 'site_name'). Defaults to 'pfp_name'.
            return_field (optional): the attribute field to return.
            Defaults to None.

        Returns:
            All attributes or requested attribute.

        """

        df = self._index_translator(use_index=source_field)
        if return_field is None:
            return df.loc[variable]
        return df.loc[variable, return_field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def translate_all_variables(self, source_field: str='site_name') -> dict:
        """
        Maps the translation between site names and pfp names.

        Args:
            source_field (optional): the source field for the variable name
            (either 'pfp_name' or 'site_name'). Defaults to 'site_name'.

        Returns:
            Dictionary containing the mapping.

        """

        translate_to = self._get_target_field_from_source(
            source_field=source_field
            )
        return (
            self._index_translator(use_index=source_field)
            [translate_to]
            .to_dict()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def translate_variables_by_table(
            self, table: str, source_field: str='site_name'
            ) -> dict:
        """
        Maps the translation between site names and pfp names for a specific file.

        Args:
            table: name of table for which to fetch translations.
            source_field (optional): the source field for the variable name
            (either 'pfp_name' or 'site_name'). Defaults to 'site_name'.

        Returns:
            Dictionary containing the mapping.

        """

        translate_to = self._get_target_field_from_source(
            source_field=source_field
            )
        df = self._index_translator(use_index=source_field)
        return (
            df.loc[df.table==table]
            [translate_to]
            .to_dict()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def translate_single_variable(
            self, variable: str=None, source_field: str='site_name'
            ) -> dict:
        """
        Maps the translation between site names and pfp names for a specific variable.

        Args:
            variable (optional): name of variable. Defaults to None.
            source_field (optional): the source field for the variable name
            (either 'pfp_name' or 'site_name'). Defaults to 'site_name'.

        Returns:
            Dictionary containing the mapping.

        """

        return (
            self.translate_all_variables(source_field=source_field)
            [variable]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_target_field_from_source(self, source_field):

        translate_to = self._NAME_MAP.copy()
        translate_to.pop(source_field)
        return list(translate_to.values())[0]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _index_translator(self, use_index: str):

        return (
            self.variable_lookup_table
            .reset_index()
            .set_index(keys=self._NAME_MAP[use_index])
            )
    #--------------------------------------------------------------------------

    ###########################################################################
    ### END VARIABLE DESCRIPTORS SECTION ###
    ###########################################################################

#------------------------------------------------------------------------------



#------------------------------------------------------------------------------
class PFPNameParser():
    """Tool that:
        1) defines pfp standard variables and attributes (names, units);
        2) allows retrieval of attributes of standard pfp variables;
        3) provides conformity checking for names in site-based configurations,
           according to the below rules.

    PFP names are composed of descriptive substrings separated by underscores.

    1) First component MUST be a unique variable identifier. The list of
    currently defined variable identifiers can be accessed as a class attribute
    (self.valid_variable_identifiers).

    2) Second component can be either a unique device identifier (listed under
    VALID_DEVICES in this module) or a location identifier. The former are
    variables that are either unique to that device or that contain attribute
    ambiguities for the same variable identifier (e.g. Tv_SONIC_)


    """

    #--------------------------------------------------------------------------
    def __init__(self):
        """
        Load the naming info from the std_names yaml, and create the lookup
        tables and reference objects.

        Returns:
            None.

        """

        # Load yaml and create attribute lookup table
        rslt = cm.get_global_configs(which='pfp_std_names')
        self.lookup_table = pd.DataFrame(rslt).T.rename_axis('pfp_name')

        # List of valid variable identifier substrings
        self.valid_variable_identifiers = (
            np.unique(
                [var.split('_')[0] for var in self.lookup_table.index]
                )
            .tolist()
            )

        # Dict of device names with valid variables (variables that may contain
        # the device name i.e. SONIC or IRGA)
        self.valid_device_variables = {
            device: np.unique(
                [
                    var.split('_')[0] for var in self.lookup_table.index
                    if device in var
                    ]
                ).tolist()
            for device in VALID_DEVICES
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_standard_variables(self):

        return self.lookup_table.index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attributes(
            self, variable_name: str, return_field: str=None
            ) -> pd.Series | str:
        """
        Return attributes of variable. Note that the naming convention does NOT
        have to exactly match the pfp_variable (e.g. Ta_2m_Av can be passed
        and will retrieve Ta attributes), but it MUST conform to the pfp
        variable naming standards.

        Args:
            pfp_name: name for which to return attributes.
            return_field (optional): specific attribute to return. Defaults to None.

        Returns:
            Attribute(s).

        """

        rslt = self.get_variable_name_components(variable_name=variable_name)
        rslt_list = list(filter(lambda x: isinstance(x, str), rslt.values()))
        while True:
            try:
                attrs = self.lookup_table.loc['_'.join(rslt_list)]
                break
            except KeyError as e:
                rslt_list.remove(rslt_list[-1])
                if len(rslt_list) == 0:
                    raise KeyError(
                        f'No entry found for any variants of {variable_name}'
                        ) from e
        if return_field is None:
            return attrs
        return attrs[return_field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_device_variables(self, device: str=None) -> list:

        if not device is None:
            return self.valid_device_variables[device]
        return sum([x for x in self.valid_device_variables.values()], [])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_name_components(self, variable_name: str):
        """
        Parse the variable name string for conformity

        Args:
            variable_name (str): DESCRIPTION.

        Raises:
            KeyError: raised if any slots contain unrecognised variables,
            devices or processes.
            IndexError: raised if too many slots (>3).

        Returns:
            None.

        """

        # Parse the string components
        rslt = {}
        for i, parse_str in enumerate(variable_name.split('_')):

            # First slot must contain a variable identifier
            if i == 0:
                rslt['variable'] = self._check_str_is_variable(
                    parse_str=parse_str
                    )

            # Second slot must contain either a device or location identifier;
            # if it is a device, it must have a valid variable identifier for
            # the device (e.g. Sws_SONIC is not okay).
            if i == 1:
                try:
                    rslt['device'] = self._check_str_is_device(
                        parse_str=parse_str, var_id=rslt['variable']
                        )
                    continue
                except TypeError:
                    raise
                except KeyError as e:
                    first_error = e.args[0]
                    rslt['device'] = None
                try:
                    rslt.update(
                        self._check_str_is_location(parse_str=parse_str)
                        )
                except (KeyError, TypeError) as e:
                    second_error = e.args[0]
                    raise Exception(
                        'Neither a valid device or location was found! \n'
                        f'{first_error}\n'
                        f'{second_error}'
                        )

            # Third slot must contain a process identifier
            if i == 2:
                rslt['process'] = self._check_str_is_process(
                    parse_str=parse_str
                    )

            # No fourth slot allowed!
            if i == 3:
                raise IndexError('Too many slots!')

        return rslt
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_variable(self, parse_str: str) -> str:

        if parse_str in self.valid_variable_identifiers:
            return parse_str
        raise KeyError(
            f'{parse_str} is not a valid variable identifier'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_device(self, parse_str: str, var_id: str) -> str:

        try:
            valid_vars = self.valid_device_variables[parse_str]
            if var_id in valid_vars:
                return parse_str
            raise TypeError(
                f'Variable {var_id} is not valid for device {parse_str}'
                )
        except KeyError:
            raise KeyError(
                f'{parse_str} is not a valid device identifier'
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_process(self, parse_str: str) -> str:

        if parse_str in VALID_SUFFIXES.keys():
            return parse_str
        raise KeyError(
            f'{parse_str} is not a valid process identifier'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_location(self, parse_str: str):
        """


        Args:
            parse_str (str): DESCRIPTION.

        Returns:
            bool: DESCRIPTION.

        """

        def parse_single_char(char: str) -> bool:

            if not char.isalpha:
                raise KeyError(
                    'Replicate identifier must be single alphabet character '
                    f'(you passed {char})'
                    )
            if char in VALID_LOC_UNITS:
                raise KeyError(
                    'Replicate identifier must not be a height / depth unit '
                    f'(you passed {char})'
                    )
            return char

        # Parse single character str
        if len(parse_str) == 1:
            return {
                'location': None,
                'replicate': parse_single_char(char=parse_str)
                }

        # Parse multicharacter str (must have height units embedded, these
        # units must be )
        for units in VALID_LOC_UNITS:
            if units in parse_str:
                parse_list = parse_str.split(units)
                if not parse_list[0].isdigit():
                    raise TypeError(
                        'Location identifier units must be preceded by integer'
                        )
                if len(parse_list[1]) == 0:
                    return {
                        'location': parse_list[0] + units,
                        'replicate': None
                        }
                if len(parse_list[1]) == 1:
                    return {
                        'location': parse_list[0] + units,
                        'replicate': parse_single_char(char=parse_list[1])
                        }
        raise KeyError(
            'Multi-character location identifiers must contain height / depth '
            'units'
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### BEGIN SITE-SPECIFIC HARDWARE CONFIGURATION FUNCTIONS ###
###############################################################################

#--------------------------------------------------------------------------
def get_logger_list(site: str) -> list:
    """
    Get list of loggers.

    Args:
        site: name of site for which to return logger list.

    Returns:
        List of loggers.

    """

    hardware_configs = cm.get_site_configs(site=site, which='hardware')
    return list(hardware_configs['loggers'].keys())
#--------------------------------------------------------------------------

#------------------------------------------------------------------------------
def map_logger_tables_to_files(
        site:str, logger:str=None, raise_if_no_file: bool=True
        ) -> pd.Series:
    """
    Tie table names to local file locations. Note that this assumes that
    file nomenclature rules have been followed.

    Args:
        site: name of site for which to return map.
        logger: logger name for which to provide mapping.
        raise_if_no_file: raise exception if the file does not exist. Defaults to True.

    Raises:
        FileNotFoundError: DESCRIPTION.

    Returns:
        Series mapping logger and table to absolute file path (value).

    """

    hardware_configs = cm.get_site_configs(site=site, which='hardware')
    data_path = paths.get_local_stream_path(
        site=site, resource='data', stream='flux_slow'
        )
    logger_list = list(hardware_configs['loggers'].keys())
    if not logger is None:
        logger_list = [logger]
    rslt_list = []
    for this_logger in logger_list:
        for this_table in hardware_configs['loggers'][this_logger]['tables']:
            rslt_list.append(
                {
                    'logger': this_logger,
                    'table': this_table,
                    'file': f'{site}_{this_logger}_{this_table}.dat'
                 }
            )
    df = pd.DataFrame(rslt_list).set_index(keys=['logger', 'table'])
    abs_path_list = []
    if raise_if_no_file:
        for record in df.index:
            file = df.loc[record, 'file']
            try:
                abs_path = pathlib.Path(data_path / file)
            except TypeError:
                breakpoint()
            if not abs_path.exists():
                raise FileNotFoundError(
                    f'No file named {file} exists for table {record[0]}!'
                    )
            abs_path_list.append(abs_path)
    return df.assign(path=abs_path_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_modem_details(site: str, field: str=None) -> pd.Series | str:
    """
    Get details of modem.

    Args:
        site: name of site for which to return modem details.
        field: field to return. Defaults to None.

    Returns:
        Details of modem.

    """

    hardware_configs = cm.get_site_configs(site=site, which='hardware')
    modem_fields = pd.Series(hardware_configs['modem'])
    if field is None:
        return modem_fields
    return modem_fields[field]
#------------------------------------------------------------------------------

###############################################################################
### END SITE-SPECIFIC HARDWARE CONFIGURATION FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN SITE-SPECIFIC VARIABLE CONFIGURATION FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def make_variable_lookup_table(site):
    """
    Build a lookup table that maps variables to attributes
    (pfp_name as index).

    Args:
        site: name of site for which to return variable lookup table.

    Returns:
        Table containing the mapping.

    """

    # Make the basic variable table
    variable_configs = cm.get_site_configs(site=site, which='variables')
    vars_df = pd.DataFrame(variable_configs).T
    vars_df.index.name = 'pfp_name'

    # Make the file mapping table
    file_lookup_table = map_logger_tables_to_files(site=site)
    columns = ['file', 'path']
    files_df = (
        pd.DataFrame(
            [
                file_lookup_table.loc[x, columns] for x in
                zip(vars_df.logger.tolist(), vars_df.table.tolist())
                ],
            columns=columns
            )
        .set_index(vars_df.index)
        )

    # Make the standard naming table
    var_parser = PFPNameParser()
    names_df = (
        pd.DataFrame(
            [
                var_parser.get_variable_attributes(variable_name=var)
                for var in vars_df.index
                ]
            )
        .set_index(vars_df.index)
        )

    # Concatenate
    return pd.concat(
        [vars_df, files_df, names_df],
        axis=1
        )
#------------------------------------------------------------------------------

###############################################################################
### END SITE-SPECIFIC VARIABLE CONFIGURATION FUNCTIONS ###
###############################################################################
