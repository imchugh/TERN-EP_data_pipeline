# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 15:29:34 2024

@author: jcutern-imchugh

Todo:
    - tighten up the logic on the pfp name parser
    (currently won't allow process identifiers in slot 2);
    - pfp naming convention should provide a separate slot for replicates!

"""

import pandas as pd
import pathlib

import utils.configs_manager as cm
import file_handling.file_io as io
from sparql_site_details import site_details


paths = cm.PathsManager()
VALID_INSTRUMENTS = ['SONIC', 'IRGA', 'RAD']
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
    def __init__(self, site: str, variable_map: str='pfp'):
        """
        Do inits - read the yaml files, build lookup tables.

        Args:
            site: name of site.
            variable_map (optional): which variable configuration
            ('pfp' or 'vis') to use. Defaults to 'pfp'.

        Returns:
            None.

        """

        # Set basic attrs
        self.site = site
        self.variable_map = variable_map
        self.site_details = site_details().get_single_site_details(site=site)
        self.data_path = (
            paths.get_local_stream_path(
                site=site, resource='data', stream='flux_slow'
                )
            )

        # Create global standard variables table
        self.standard_variables = (
            pd.DataFrame(cm.get_global_configs(which='pfp_std_names'))
            .T
            .rename_axis('quantity')
            )

        self.site_variables = (
            self._get_site_variable_map()
            .pipe(self._test_variable_conformity)
            .pipe(self._map_tables_to_files)
            .pipe(self._get_standard_attributes)
            )

        # Get flux instrument types
        self.irga_type = self._get_inst_type('IRGA')
        self.sonic_type = self._get_inst_type('SONIC')

        # Private inits
        self._NAME_MAP = {'site_name': 'name', 'std_name': 'std_name'}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_site_variable_map(self):

        return (
            pd.DataFrame(
                cm.get_site_variable_configs(
                    site=self.site, which=self.variable_map
                    )
                )
            .T
            .rename_axis('std_name')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _test_variable_conformity(self, df):

        name_parser = PFPNameParser()
        test = (
            pd.DataFrame(
                [
                    name_parser.parse_variable_name(variable_name=variable_name)
                    for variable_name in df.index
                    ]
                )
            .set_index(df.index)
            .fillna('')
            .drop('instrument', axis=1)
            )
        return pd.concat([df, test], axis=1)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_standard_attributes(self, df):

        test = (
            self.standard_variables
            .loc[df.quantity]
            .set_index(df.index)
            )
        return pd.concat([df, test], axis=1)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _map_tables_to_files(self, df):

        file_lookup_table = map_logger_tables_to_files(site=self.site)
        files_df = (
            file_lookup_table.loc[zip(df.logger.tolist(), df.table.tolist())]
            .set_index(df.index)
            )
        return pd.concat([df, files_df], axis=1)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_inst_type(self, inst: str) -> str:
        """
        Get the instrument types for SONIC and IRGA.

        Args:
            inst: generic name of instrument to return (must be irga or sonic).

        Raises:
            RuntimeError: raised if more than one instrument type found.

        Returns:
            instrument type description.

        """

        var_list = [x for x in self.site_variables.index if inst in x]
        inst_list = self.site_variables.loc[var_list].instrument.unique()
        if not len(inst_list) == 1:
            raise RuntimeError(
                'More than one instrument specified as instrument attribute '
                f'for {inst} device variable ({", ".join(inst_list)})'
                )
        return inst_list[0]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_attributes(self, file: str) -> pd.core.series.Series:
        """


        Args:
            file: file for which to get attributes.

        Returns:
            series of attributes.

        """

        return pd.Series(
            io.get_file_info(file=self.data_path / file) |
            io.get_start_end_dates(file=self.data_path / file)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_tables(self) -> list:
        """
        List the tables defined in the variable map.

        Returns:
            the list of tables.

        """

        return self.site_variables.table.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_files(self) -> list:
        """
        List the files constructed from the site, logger and table attributes
        of the variable map.

        Returns:
            the list of files.

        """

        return self.site_variables.file.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_variables(self) -> list:
        """
        List the variables defined in the configuration file.

        Returns:
            the list of variables.

        """

        return self.site_variables.index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_variables_for_conversion(self) -> list:
        """
        List the variables defined in the configuration file that have
        site units that differ from standard units.

        Returns:
            the list of variables.

        """

        return (
            self.site_variables.loc[
                self.site_variables.units!=
                self.site_variables.standard_units
                ]
            .index
            .tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_tables_to_files(
        self, table: str | list=None, abs_path: bool=False
        ) -> dict:
        """


        Args:
            table (optional): table for which to return file. If None, maps all
            available tables to files. Defaults to None.
            abs_path (optional): whether to return the absolute path (if True)
            or just the file name (if False). Defaults to False.

        Returns:
            the map.

        """

        if table is None:
            table = self.list_tables()

        s = (
            pd.Series(
                data=self.site_variables.file.tolist(),
                index=self.site_variables.table.tolist()
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
            self, variable: str, source_field: str='std_name',
            return_field: str=None
            ) -> pd.Series | str:
        """
        Get the attributes for a given variable

        Args:
            variable: the variable for which to return the attribute(s).
            source_field (optional): the source field for the variable name
            (either 'std_name' or 'site_name'). Defaults to 'std_name'.
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
            (either 'std_name' or 'site_name'). Defaults to 'site_name'.

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
            (either 'std_name' or 'site_name'). Defaults to 'site_name'.

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
            (either 'std_name' or 'site_name'). Defaults to 'site_name'.

        Returns:
            Dictionary containing the mapping.

        """

        return (
            self.translate_all_variables(source_field=source_field)
            [variable]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_target_field_from_source(self, source_field: str) -> str:
        """
        Get the gets the inverse of the source field.

        Args:
            source_field: field for which to retrieve inverse
            (if 'site_name' is source, 'std_name' is return, and vice versa).

        Returns:
            inverse return string.

        """

        translate_to = self._NAME_MAP.copy()
        translate_to.pop(source_field)
        return list(translate_to.values())[0]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _index_translator(self, use_index: str) -> pd.core.frame.DataFrame:
        """
        Reindex the variable lookup table on the desired index.

        Args:
            use_index: name of index to use.

        Returns:
            reindexed dataframe.

        """

        return (
            self.site_variables
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
    VALID_INSTRUMENTS in this module) or a location identifier. The former are
    variables that are either unique to that device or that contain attribute
    ambiguities for the same variable identifier (e.g. Tv_SONIC_)


    """

    #--------------------------------------------------------------------------
    def __init__(self):
        """
        Load the naming info from the std_names yml, and create the lookup
        tables and reference objects.

        Returns:
            None.

        """

        self.split_char = '_'

        # Load yaml and create attribute lookup table
        # rslt = cm.get_global_configs(which='pfp_std_names')
        # self.lookup_table = pd.DataFrame(rslt).T.rename_axis('quantity')
        self.variable_list = (
            list(cm.get_global_configs(which='pfp_std_names').keys())
            )

        # List of valid instrument identifiers
        # self.valid_instrument_identifiers = VALID_INSTRUMENTS

        # # List of valid location units
        # self.valid_location_identifiers = VALID_LOC_UNITS

        # # List of valid processes
        # self.valid_process_identifiers = list(VALID_SUFFIXES.keys())

        # # Dict of device names with valid variables (variables that may contain
        # # the device name i.e. SONIC or IRGA)
        # self.valid_instrument_variable_identifiers = {
        #     instrument: (
        #         pd.Series(
        #             [
        #                 var.split('_')[0] for var in self.lookup_table.index
        #                 if instrument in var
        #                 ]
        #             )
        #         .unique()
        #         .tolist()
        #         )
        # for instrument in VALID_INSTRUMENTS
        # }
    #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def list_standard_variables(self):

    #     return self.lookup_table.index.tolist()
    # #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def get_standard_variable_attributes(self, variable_name):

    #     quantity = (
    #         self._extract_quantity(
    #             parse_list=variable_name.split(self.split_char)
    #             )
    #         ['quantity']
    #         )
    #     idx = self.lookup_table.index.get_loc(quantity)
    #     return self.lookup_table.reset_index().iloc[idx]
    # #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def parse_variable_name(self, variable_name):

        rslt_dict = {
            'quantity': None,
            'instrument': None,
            'location': None,
            'replicate': None,
            'process': None
            }

        # Get string elements
        elems = variable_name.split('_')

        # Find quantity and instrument (if valid)
        rslt_dict.update(self._check_str_is_quantity(elems))

        # Return if list exhausted
        if len(elems) == 0:
            return rslt_dict

        # Check for replicates / locations
        try:
            rslt_dict.update(self._check_str_is_num_replicate(parse_list=elems))
        except TypeError:
            try:
                rslt_dict.update(self._check_str_is_location(parse_list=elems))
            except TypeError:
                pass

        # Return if list exhausted
        if len(elems) == 0:
            return rslt_dict

        # Check for process
        rslt_dict.update(self._check_str_is_process(parse_list=elems))

        # Return if list exhausted
        if len(elems) == 0:
            return rslt_dict

        # Raise error if list elements remain
        raise RuntimeError('Process identifier must be the final element!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_quantity(self, parse_list: str) -> dict:

        quantity = parse_list[0]
        instrument = None
        parse_list.remove(quantity)
        if not len(parse_list) == 0:
            if parse_list[0] in VALID_INSTRUMENTS:
                quantity = '_'.join([quantity, parse_list[0]])
                instrument = parse_list[0]
                parse_list.remove(instrument)
        if not quantity in self.variable_list:
            raise KeyError(
                'Not a valid quantity identifier!'
                )
        return {'quantity': quantity, 'instrument': instrument}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_num_replicate(self, parse_list: str) -> dict:

        digit = parse_list[0]
        if digit.isdigit():
            parse_list.remove(digit)
            return {'replicate': digit}
        raise TypeError('Not a number!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_location(self, parse_list):

        parse_str = parse_list[0]

        for units in VALID_LOC_UNITS:

            sub_list = parse_str.split(units)
            if len(sub_list) == 1:
                error = (
                    'No recognised height / depth units in location '
                    'identifier!'
                    )
                continue

            if not sub_list[0].isdigit():
                error = (
                    'Characters preceding height / depth units must be '
                    'numeric'
                    )
                continue

            if len(sub_list[1]) != 0:
                if not sub_list[1].isalpha:
                    error = (
                        'Characters succeeding valid location identifier '
                        'must be single alpha character'
                        )
                    continue

            parse_list.remove(parse_str)
            location = ''.join([sub_list[0], units])
            replicate = None
            if len(sub_list[1]) != 0:
                replicate = sub_list[1]
            return {'location': location, 'replicate': replicate}

        raise TypeError(error)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_process(self, parse_list: list) -> dict:

        process = parse_list[0]
        if not process in VALID_SUFFIXES.keys():
            raise KeyError(
                f'{parse_list[0]} is not a valid process identifier'
                )
        parse_list.remove(process)
        return {'process': process}
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### BEGIN SITE-SPECIFIC HARDWARE CONFIGURATION FUNCTIONS ###
###############################################################################

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

    hardware_configs = cm.get_site_hardware_configs(site=site, which='logger')
    data_path = paths.get_local_stream_path(
        site=site, resource='data', stream='flux_slow'
        )
    logger_list = list(hardware_configs.keys())
    if not logger is None:
        logger_list = [logger]
    rslt_list = []
    for this_logger in logger_list:
        for this_table in hardware_configs[this_logger]['tables']:
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
            abs_path = pathlib.Path(data_path / file)
            if not abs_path.exists():
                raise FileNotFoundError(
                    f'No file named {file} exists for table {record[0]}!'
                    )
            abs_path_list.append(abs_path)
    return df.assign(path=abs_path_list)
#------------------------------------------------------------------------------

###############################################################################
### END SITE-SPECIFIC HARDWARE CONFIGURATION FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN SITE-SPECIFIC VARIABLE CONFIGURATION FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def make_variable_lookup_table(site, variable_map):
    """
    Build a lookup table that maps variables to attributes
    (std_name as index).

    Args:
        site: name of site for which to return variable lookup table.

    Returns:
        Table containing the mapping.

    """

    # Make the basic variable table
    variable_configs = cm.get_site_variable_configs(
        site=site, which=variable_map
        )
    vars_df = pd.DataFrame(variable_configs).T
    vars_df.index.name = 'std_name'

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
                var_parser.get_standard_variable_attributes(variable_name=var)
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

if __name__=='__main__':

    md_mngr = MetaDataManager(site='Calperum')