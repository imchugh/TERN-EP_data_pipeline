# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 15:29:34 2024

@author: jcutern-imchugh

Todo:
    - tighten up the logic on the pfp name parser
    (currently won't allow process identifiers in slot 2);
    - pfp naming convention should provide a separate slot for replicates!
    - pfp name parser currently hacky because we need to handle variances.
    This complicates the parsing because the units are different. We should
    ditch variances.

"""

import pandas as pd

import utils.configs_manager as cm
import file_handling.file_io as io
from sparql_site_details import site_details


paths = cm.PathsManager()
SPLIT_CHAR = '_'
VALID_INSTRUMENTS = ['SONIC', 'IRGA', 'RAD']
VALID_LOC_UNITS = ['cm', 'm']
VALID_SUFFIXES = {
    'Av': 'average', 'Sd': 'standard deviation', 'Vr': 'variance', 'Sum': 'Sum'
    }

###############################################################################
### BEGIN SINGLE SITE CONFIGURATION READER SECTION ###
###############################################################################

#------------------------------------------------------------------------------
class MetaDataManager():
    """Class to read and interrogate variable data from site-specific config file"""

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def __init__(self, site: str, variable_map: str='pfp') -> None:
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
        self.data_path = (
            paths.get_local_stream_path(
                site=site, resource='data', stream='flux_slow'
                )
            )

        # Create global standard variables table
        self.standard_variables = (
            pd.DataFrame(cm.get_global_configs(which='pfp_std_names'))
            .T
            .rename_axis('variable')
            )

        # Create site-based variables table
        self.site_variables = (
            self._get_site_variable_map()
            .pipe(self._test_variable_conformity)
            .pipe(self._map_table_to_file)
            )

        # Get flux instrument types
        self.instruments = self._get_inst_type()

        # Private inits
        self._NAME_MAP = {'site_name': 'name', 'std_name': 'std_name'}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_site_variable_map(self) -> pd.DataFrame:
        """
        Get the site variable names / attributes.

        Returns:
            dataframe containing variable names as index and attributes as cols.

        """

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
    def _test_variable_conformity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check all names in the configuration file conform to pfp standards.

        Args:
            df: dataframe containing variable names in index.

        Returns:
            returns data unaltered or raises exception if any non-compliant vars.

        """

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
            )
        return pd.concat([df, test], axis=1)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _map_table_to_file(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate standard file names based on site, logger and table names.

        Args:
            df: the dataframe to which to append the information.

        Returns:
            the dataframe with additional information.

        """

        df['file'] = list(map(
            lambda x: f'{self.site}_{x[0]}_{x[1]}.dat',
            zip(df.logger.tolist(), df.table.tolist())
            ))
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_inst_type(self) -> str:
        """
        Get the instrument types for SONIC and IRGA.

        Raises:
            RuntimeError: raised if more than one instrument type found.

        Returns:
            instrument type description.

        """

        rslt = {}
        for instrument in ['SONIC', 'IRGA']:
            var_list = [x for x in self.site_variables.index if instrument in x]
            inst_list = self.site_variables.loc[var_list].instrument.unique()
            if not len(inst_list) == 1:
                raise RuntimeError(
                    'More than one instrument specified as instrument attribute '
                    f'for {instrument} device variable ({", ".join(inst_list)})'
                    )
            rslt.update({instrument: inst_list[0]})
        return rslt
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_details(self) -> pd.Series:
        """
        Get the non-variable site details.

        Returns:
            The details.

        """

        return site_details().get_single_site_details(site=self.site)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_attributes(
            self, file: str, include_extended=False
            ) -> pd.Series:
        """
        Get file attributes from file header.

        Args:
            file: file for which to get attributes.

        Returns:
            attributes.

        """

        rslt = (
            io.get_file_info(file=self.data_path / file) |
            io.get_start_end_dates(file=self.data_path / file)
            )
        if include_extended:
            rslt.update(
                {'interval': io.get_file_interval(self.data_path / file)} |
                {'backups': io.get_eligible_concat_files(self.data_path / file)}
                )
        return pd.Series(rslt)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_loggers(self) -> list:

        return self.site_variables.logger.unique().tolist()
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
    def list_files(self, abs_path=False) -> list:
        """
        List the files constructed from the site, logger and table attributes
        of the variable map.

        Returns:
            the list of files.

        """
        the_list = self.site_variables.file.unique().tolist()
        if not abs_path:
            return the_list
        return [self.data_path / file for file in the_list]
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
    def map_loggers_to_tables(self) -> dict:
        """
        Return a dictionary that maps loggers to table names.

        Args:
            table (optional): table for which to return file. If None, maps all
            available tables to files. Defaults to None.

        Returns:
            the map.

        """


        sub_df = (
            self.site_variables[['logger', 'table']]
            .reset_index(drop=True)
            .drop_duplicates()
            )
        return {x[0]: x[1].table.tolist() for x in sub_df.groupby('logger')}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_tables_to_files(
        self, table: str | list=None, abs_path: bool=False
        ) -> dict:
        """
        Return a dictionary that maps logger tables to file names.

        Args:
            table (optional): table for which to return file. If None, maps all
            available tables to files. Defaults to None.
            abs_path (optional): whether to return the absolute path (if True)
            or just the file name (if False). Defaults to False.

        Returns:
            the map.

        """

        if isinstance(table, str):
            table = [table]

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
            self, table: str=None, source_field: str='site_name'
            ) -> dict:
        """
        Maps the translation between site names and pfp names for a specific
        table.

        Args:
            table (optional): name of table for which to fetch translations.
            Defaults to None.
            source_field (optional): the source field for the variable name
            (either 'std_name' or 'site_name'). Defaults to 'site_name'.

        Returns:
            Dictionary containing the mapping.

        """

        return self._translate_by_something(
            translate_by='table',
            file_or_table=table,
            source_field=source_field
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def translate_variables_by_file(
            self, file: str=None, source_field: str='site_name',
            abs_path: bool=False
            ):
        """
        Maps the translation between site names and pfp names for a specific
        file.

        Args:
            file (optional): name of file for which to fetch translations.
            Defaults to None.
            source_field (optional): the source field for the variable name
            (either 'std_name' or 'site_name'). Defaults to 'site_name'.

        Returns:
            Dictionary containing the mapping.

        """

        rslt = self._translate_by_something(
            translate_by='file',
            file_or_table=file,
            source_field=source_field
            )
        if not abs_path:
            return rslt
        return {
            self.data_path / the_file: the_map
            for the_file, the_map in rslt.items()
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _translate_by_something(
            self, translate_by: str, file_or_table: str=None,
            source_field: str='site_name',
            ):
        """


        Args:
            translate_by: group translation maps by either file or table.
            file_or_table (optional): pass a valid file or table for
            specific translation map. Defaults to None.
            source_field (optional): the source field for the variable name
            (either 'std_name' or 'site_name'). Defaults to 'site_name'.

        Returns:
            Dictionary containing the mapping.

        """

        translate_to = self._get_target_field_from_source(
            source_field=source_field
            )
        df = self._index_translator(use_index=source_field)
        if file_or_table is not None:
            return (
                df.loc[df[translate_by]==file_or_table]
                [translate_to]
                .to_dict()
                )
        return {
            grp[0]: grp[1][translate_to].to_dict()
            for grp in df.groupby(translate_by)
            }
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
        Gets the inverse of the source field.

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
    """Tool that checks names in site-based configuration files conform to pfp
    rules.

    PFP names are composed of descriptive substrings separated by underscores.

    1) First component MUST be a unique variable identifier. The list of
    currently defined variable identifiers can be accessed as a class attribute
    (`self.variable_list`).

    2) Second component can be either a unique device identifier (listed under
    VALID_INSTRUMENTS in this module) or a location identifier. The former are
    variables that are either unique to that device or that would be ambiguous
    in the absence of a device identifier (e.g. AH versus AH_IRGA).

    3) If there is a process identifier, it must occur as the last substring.
    If it is 'Vr', this must become past of the unique variable identifier,
    because variances necessarily have different units.

    """

    #--------------------------------------------------------------------------
    def __init__(self):
        """
        Load the naming info from the `std_names` yml.

        Returns:
            None.

        """

        self.variables = (
            pd.DataFrame(cm.get_global_configs(which='pfp_std_names'))
            .T
            .rename_axis('quantity')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def parse_variable_name(self, variable_name: str) -> dict:
        """
        Break variable name into components and parse each for conformity.

        Args:
            variable_name: the complete variable name.

        Raises:
            RuntimeError: raised if number of elements in substring list is
            wrong.

        Returns:
            rslt_dict: the identities of the substrings.

        """

        rslt_dict = {
            'quantity': None,
            'instrument_type': None,
            'location': None,
            'replicate': None,
            'process': None
            }

        errors = []

        # Get string elements
        elems = variable_name.split(SPLIT_CHAR)

        # Find quantity and instrument (if valid)
        rslt_dict.update(self._check_str_is_quantity(elems))

        # Check if final element is a process
        try:
            rslt_dict.update(self._check_str_is_process(parse_list=elems))
        except TypeError as e:
            errors.append(e.args[0])

        # Check how many elements remain. If more than one, throw an error
        if len(elems) > 1:
            raise RuntimeError('Too many elements!')

        # Check if next element is a replicate
        try:
            rslt_dict.update(
                self._check_str_is_alphanum_replicate(parse_list=elems)
                )
        except TypeError as e:
            errors.append(e.args[0])

        # Check if next element is a location / replicate combo
        try:
            rslt_dict.update(self._check_str_is_location(parse_list=elems))
        except TypeError as e:
            errors.append(e.args[0])

        # Raise error if list element remains
        if len(elems) > 0:
            raise RuntimeError(
                'Unrecognised element remains: checks failed with the '
                f'following messages: {errors}'
                )

        # Get properties
        rslt_dict.update(self.variables.loc[rslt_dict['quantity']].to_dict())
        if rslt_dict['process'] == 'Vr':
            rslt_dict['standard_units'] = (
                convert_units_to_variance(units=rslt_dict['standard_units'])
                )

        # Return results
        return rslt_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_quantity(self, parse_list: str) -> dict:
        """
        Check the list of elements for quantity substrings.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            KeyError: raised if a valid quantity identifier not found.

        Returns:
            the identities of the substrings.

        """

        # Inits
        quantity = parse_list[0]
        parse_list.remove(quantity)
        instrument = None

        # Check for an instrument identifier
        if not len(parse_list) == 0:
            if parse_list[0] in VALID_INSTRUMENTS:
                quantity = '_'.join([quantity, parse_list[0]])
                instrument = parse_list[0]
                parse_list.remove(instrument)
        if not quantity in self.variables.index:
            raise KeyError(
                f'{quantity} is not a valid quantity identifier!'
                )

        return {
            'quantity': quantity,
            'instrument_type': instrument,
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_process(self, parse_list: str) -> dict:

        if len(parse_list) == 0:
            return {}
        if not parse_list[-1] in VALID_SUFFIXES.keys():
            raise TypeError(
                f'{parse_list[-1]} is not a valid process identifier!'
                )
        process = parse_list[-1]
        parse_list.remove(process)
        return {'process': process}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_alphanum_replicate(self, parse_list: str) -> dict:
        """
        Check the list of elements for alphanumeric replicate substrings.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            TypeError: raised if not a single alphanumeric character.

        Returns:
            the identities of the substrings.

        """

        if len(parse_list) == 0:
            return {}

        elem = parse_list[0]
        if len(elem) == 1:
            if elem.isdigit() or elem.isalpha():
                parse_list.remove(elem)
                return {'replicate': elem}
        raise TypeError(
            'Replicate identifier must be a single alphanumeric character! '
            f'You passed "{elem}"!'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_location(self, parse_list: str) -> dict:
        """
        Check the list of elements for location / replicate substrings.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            TypeError: raised if substring non-conformal.

        Returns:
            the identities of the substrings.

        """

        if len(parse_list) == 0:
            return {}

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
                    'numeric!'
                    )
                continue

            if len(sub_list[1]) != 0:
                if not sub_list[1].isalpha():
                    error = (
                        'Characters succeeding valid location identifier '
                        'must be single alpha character!'
                        )
                    continue

            parse_list.remove(parse_str)
            location = ''.join([sub_list[0], units])
            replicate = None
            if len(sub_list[1]) != 0:
                replicate = sub_list[1]
            return {'location': location, 'replicate': replicate}

        raise TypeError(
            error + f' Passed substring "{parse_str}" does not conform!'
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_units_to_variance(units):

    ref_dict = {
        'g/m^3': 'g^2/m^6',
        'umol/mol': 'umol/mol',
        'mg/m^3': 'mg^2/m^6',
        'degC': 'degC^2',
        'm/s': 'm^2/s^2'
        }

    return ref_dict[units]
#------------------------------------------------------------------------------
