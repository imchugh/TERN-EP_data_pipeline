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

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import pathlib
import pandas as pd

#------------------------------------------------------------------------------

from managers import paths
from file_handling import file_io as io
from managers.site_details import SiteDetailsManager as sdm

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

VALID_INSTRUMENTS = ['SONIC', 'IRGA', 'RAD']
VALID_FLUX_SYSTEMS = {'EF': 'EasyFluxDL', 'EP': 'SmartFlux', 'DL': 'TERN'}
TURBULENT_FLUX_QUANTITIES = ['Fco2', 'Fe', 'Fh']
FLUX_FILE_VAR_IND = 'Fco2'
VALID_LOC_UNITS = ['cm', 'm']
VALID_SUFFIXES = {
    'Av': 'average', 'Sd': 'standard_deviation', 'Vr': 'variance',
    'Sum': 'sum', 'Ct': 'count', 'QC': 'quality_control_flag'
    }
_NAME_MAP = {'site_name': 'name', 'std_name': 'std_name'}

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN SITE METADATA MANAGER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class MetaDataManager():
    """
    Class to read and interrogate variable metadata from site-specific
    config file
    """

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def __init__(
            self, site: str, use_alternate_configs: str | pathlib.Path=None
            ) -> None:
        """
        Do inits - read the yaml files, build lookup tables.

        Args:
            site: name of site.
            use_alternate_configs: alternate yml variable assignment file.

        Returns:
            None.

        """

        # Set basic attrs
        self.site = site
        self.data_path = (
        paths.get_local_stream_path(
                site=site, resource='raw_data', stream='flux_slow'
                )
            )
        self.site_details = (
            sdm(use_local=True)
            .get_single_site_details(site=self.site)
            )

        # Save the basic configs from the yml file
        if use_alternate_configs:
            self.configs = paths.get_other_config_file(
                file_path=use_alternate_configs
                )
        else:
            self.configs = paths.get_local_config_file(
                config_stream='variables_pfp', site=site
                )

        # Check for the diag_type attribute in the configurations, and set them
        # to either 'valid_count' (default in new Campbell progs) or
        # 'invalid_count' (past default)
        self.diag_types = self._parse_diagnostics()

        # Create site-based variables table
        self.site_variables = self._parse_site_variables()

        # Determine and write system type / flux file
        self.flux_file = self._get_flux_file()

        # Get flux instrument types
        self.instruments = self._parse_instrument_type()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _parse_diagnostics(self) -> dict:
        """
        Get the expression of the diag_type variable (either 'valid_count'
        [default in new Campbell progs] or 'invalid_count' [past default])
        and remove from configuration.

        Returns:
            rslt: dictionary with IRGA and SONIC entries.

        """

        rslt = {
            'Diag_IRGA': 'invalid_count',
            'Diag_SONIC': 'invalid_count'
            }
        for variable in ['Diag_IRGA', 'Diag_SONIC']:
            try:
                rslt[variable] = (
                    self.configs[variable].pop('diag_type')
                    )
            except KeyError:
                next
        return rslt
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _parse_site_variables(self) -> pd.DataFrame:
        """
        Get the site variable names / attributes.

        Returns:
            dataframe containing variable names as index and attributes as cols.

        """

        # Get the variable map and check the conformity of all names
        return (
            pd.DataFrame(self.configs)
            .T
            .rename_axis('std_name')
            .pipe(self._test_variable_conformity)
            .pipe(self._test_file_assignment)
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

        # Get the name parser, check all names and preserve additional properties
        name_parser = PFPNameParser()

        # Here we deal with the problem that PFP allows two distinct quantities
        # using the same variable name: CO2_IRGA can either be mass concentration
        # OR dry mole fraction. This should change, but we hack in the meantime
        # to ensure the correct units are assigned to the attributes.
        props_list = []
        for variable in df.index:
            parser_name = variable
            if 'CO2_IRGA' in variable:
                if 'mg' in df.loc[variable, 'units']:
                    parser_name = variable.replace('CO2', 'CO2c')
            props_list.append(
                name_parser.parse_variable_name(variable_name=parser_name)
                )

        # Concatenate the properties into df
        props_df = (
            pd.DataFrame(props_list)
            .set_index(df.index)
            .fillna('')
            )

        # Convert minima and maxima columns to numeric
        for col in ['plausible_min', 'plausible_max']:
            props_df[col] = pd.to_numeric(props_df[col])

        return pd.concat([df, props_df], axis=1)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _test_file_assignment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate standard file names based on site, logger and table names.

        Args:
            df: the dataframe to which to append the information.

        Returns:
            the dataframe with additional information.

        """

        # Make file names
        file_list = (
            f'{self.site}_' +
            df[['logger', 'table']]
            .agg('_'.join, axis=1) + '.dat'
            )

        # Check file paths are valid
        for file in file_list.unique():
            if not (self.data_path / file).exists():
                raise FileNotFoundError(
                    f'File {file} does not exist in the data path'
                    )

        # Assign and return
        return df.assign(file=file_list).fillna('')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_flux_file(self) -> str:
        """
        Use the FLUX_FILE_VAR_IND constant to set the system type and flux
        file name, logger and table.

        Raises:
            RuntimeError: raised if the suffix immediately following the
            generic flux name is not a valid flux system descriptor.
            KeyError: raised if no variables containing the constant are found
            in the mapping file.

        Returns:
            None.

        """
        var = [
            key for key, value in self.map_fluxes_to_standard_names().items()
            if value == FLUX_FILE_VAR_IND
            ]
        return self.site_variables.loc[var, 'file'].item()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _parse_instrument_type(self) -> str:
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
            if len(inst_list) > 1:
                raise RuntimeError(
                    'More than one instrument specified as instrument attribute '
                    f'for {instrument} device variable ({", ".join(inst_list)})'
                    )
            if len(inst_list) == 0:
                rslt.update({instrument: None})
            else:
                rslt.update({instrument: inst_list[0]})
        return rslt
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_file_attributes(
            self, file: str, include_backups=False, include_extended=False,
            return_field=None
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
            self._get_file_dates(file=file, include_backups=include_backups)
            )
        if include_extended:
            rslt.update(
                {'interval': io.get_file_interval(self.data_path / file)} |
                {'backups': io.get_eligible_concat_files(self.data_path / file)}
                )
        if return_field is None:
            return pd.Series(rslt)
        if isinstance(return_field, str):
            return rslt[return_field]
        if isinstance(return_field, list):
            return pd.Series(rslt).loc[return_field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_file_dates(self, file: str, include_backups: bool=False) -> dict:
        """
        Get the date span of the file(s).

        Args:
            file: file for which to retrieve span.
            include_backups (optional): If true, gets span across master file
            AND backups combined. Defaults to False.

        Returns:
            start and end dates.

        """

        file_list = [self.data_path / file]
        start_dates, end_dates = [], []
        if include_backups:
            file_list += io.get_eligible_concat_files(self.data_path / file)
        for this_file in file_list:
            dates = io.get_start_end_dates(file=this_file)
            start_dates.append(dates['start_date'])
            end_dates.append(dates['end_date'])
        return {'start_date': min(start_dates), 'end_date': max(end_dates)}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_loggers(self) -> list:
        """
        List the loggers defined in the variable map.

        Returns:
            the list of loggers.

        """

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
    def list_files(self, abs_path: pathlib.Path | str=False) -> list:
        """
        List the files constructed from the site, logger and table attributes
        of the variable map.

        Args:
            abs_path (optional): if true, output the absolute path.
            Defaults to False.

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

        return (
            self._index_translator(use_index='std_name')
            .index
            .tolist()
            )
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
    def list_variance_variables(self) -> list:
        """
        List all variance variables.

        Returns:
            list of variables.

        """

        return (
            self.site_variables
            [self.site_variables.process=='Vr']
            .index.tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_fluxes_to_standard_names(self) -> dict:
        """
        Create a dictionary to map the site-specific turbulent flux names to
        universal names. This is required because the L1 netcdf files use
        suffixes to describe the turbulent fluxes, which may need to be removed
        in certain cases.

        Returns:
            mapping from specific to general.

        """

        return (
            self.site_variables
            .quantity
            .reset_index()
            .set_index(keys='quantity')
            .loc[TURBULENT_FLUX_QUANTITIES]
            .reset_index()
            .set_index(keys='std_name')
            .squeeze()
            .to_dict()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_loggers_to_tables(self) -> dict:
        """
        Map loggers to table names.

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
        Map logger tables to file names.

        Args:
            table (optional): table for which to return file. If None, maps all
            available tables to files. Defaults to None.
            abs_path (optional): if true, return the absolute path.
            Defaults to False.

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
            self, variable: str, variance_2_stdev=False,
            source_field: str='std_name', return_field: str=None
            ) -> pd.Series | str:
        """
        Get the attributes for a given variable.

        Args:
            variable: the variable for which to return the attribute(s).
            source_field (optional): the source field for the variable name
            (either 'std_name' or 'site_name'). Defaults to 'std_name'.
            return_field (optional): the attribute field to return.
            Defaults to None.

        Returns:
            All attributes or requested attribute.

        """

        # df = self._index_translator(use_index=source_field)
        # if return_field is None:
        #     return df.loc[variable]
        # return df.loc[variable, return_field]
        series = self._index_translator(use_index=source_field).loc[variable]
        if variance_2_stdev:
            self._amend_variance_metadata(series=series)
        if return_field is None:
            return series
        return series[return_field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _amend_variance_metadata(self, series: pd.Series) -> pd.Series:

        if not series.process == 'Vr':
            return series
        series.loc['process'] = 'Sd'
        series.loc['standard_units'] = convert_variance_units(
            units=series.standard_units, to_variance=False
            )
        series.loc['statistic_type'] = 'standard_deviation'
        series.name = series.name.replace('Vr', 'Sd')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def convert_variance_attrs_2_stdev(self, variable: str) -> pd.Series:
        """
        Convert the attributes of a variance variable to standard deviation.

        Args:
            variable: name of variable.

        Returns:
            attrs: all attributes or requested attribute.

        """

        attrs = self.site_variables.loc[variable].copy()
        attrs.process = 'Sd'
        attrs.standard_units = convert_variance_units(
            units=attrs.standard_units, to_variance=False
            )
        attrs.name = attrs.name.replace('Vr', 'Sd')
        return attrs
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variables_by_quantity(self, quantity) -> list:
        """


        Args:
            quantity (TYPE): DESCRIPTION.

        Returns:
            list: DESCRIPTION.

        """

        return (
            self.site_variables.loc[self.site_variables.quantity == quantity]
            .index
            .tolist()
            )
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

        translate_to = _NAME_MAP.copy()
        translate_to.pop(source_field)
        return list(translate_to.values())[0]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _index_translator(
            self, use_index: str) -> pd.core.frame.DataFrame:
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
            .set_index(keys=_NAME_MAP[use_index])
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END SITE METADATA MANAGER CLASS ###
###############################################################################



###############################################################################
### BEGIN PFP NAME PARSER CLASS ###
###############################################################################

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
    If it is 'Vr', this must become part of the unique variable identifier,
    because variances necessarily have different units.

    """

    #--------------------------------------------------------------------------
    def __init__(self):
        """
        Load the naming info from the `std_names` yml.

        Returns:
            None.

        """

        self.SPLIT_CHAR = '_'
        self.variables = (
            pd.DataFrame(paths.get_internal_configs('pfp_std_names'))
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
            'process': None,
            'system_type': None
            }

        errors = []

        # Get string elements
        elems = variable_name.split(self.SPLIT_CHAR)

        # Find quantity and instrument (if valid)
        rslt = self._check_str_is_quantity(elems)
        if rslt_dict['quantity'] is None:
            rslt_dict.update(rslt)

        # Check if final element is a process
        try:
            rslt_dict.update(self._check_str_is_process(parse_list=elems))
        except TypeError as e:
            errors.append(e.args[0])

        # Check if first remaining element is a system type
        try:
            rslt_dict.update(self._check_str_is_system_type(parse_list=elems))
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
                'Unrecognised element remains: checks failed for variable '
                f'name {variable_name} with the following messages: {errors}'
                )

        # Get properties
        rslt_dict.update(self.variables.loc[rslt_dict['quantity']].to_dict())
        if rslt_dict['process'] == 'Vr':
            rslt_dict['standard_units'] = (
                convert_variance_units(units=rslt_dict['standard_units'])
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
        process = parse_list[-1]
        if process not in VALID_SUFFIXES.keys():
            raise TypeError(
                f'{process} is not a valid process identifier!'
                )
        parse_list.remove(process)
        return {'process': process}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_system_type(self, parse_list: list) -> dict:

        if len(parse_list) == 0:
            return {}
        sys_type = parse_list[0]
        if sys_type not in VALID_FLUX_SYSTEMS.keys():
            raise TypeError(
                f'{sys_type} is not a valid system identifier!'
                )
        parse_list.remove(sys_type)
        return {'system_type': VALID_FLUX_SYSTEMS[sys_type]}
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

        valid = False
        if len(elem) == 1:
            if elem.isdigit():
                valid = True
            if elem.isalpha():
                valid = True

        if valid:
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

            split_list = sub_list[0].split('-')
            if len(split_list) > 2:
                error = (
                    'A maximum of two location idenitifers is allowed!'
                    )
            try:
                [float(s) for s in split_list]
            except ValueError:
                error = (
                    'Characters preceding height / depth units must be '
                    'numeric, or contain numerals separated by single "-"!'
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

###############################################################################
### END PFP NAME PARSER CLASS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def convert_variance_units(units: str, to_variance=True) -> str:
    """
    Convert standard units to variance.

    Args:
        units: the units.

    Returns:
        altered units.

    """

    ref_dict = {
        'g/m^3': 'g^2/m^6',
        'umol/mol': 'umol/mol',
        'mg/m^3': 'mg^2/m^6',
        'degC': 'degC^2',
        'm/s': 'm^2/s^2'
        }

    if not to_variance:
        ref_dict = {value: key for key, value in ref_dict.items()}
    return ref_dict[units]
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
