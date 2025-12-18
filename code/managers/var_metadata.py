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
from typing import Optional, Dict, ClassVar, List
from pydantic import BaseModel, model_validator, field_validator
import yaml

#------------------------------------------------------------------------------

from file_handling import file_io as io
from managers import paths

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

VALID_INSTRUMENTS = ['SONIC', 'IRGA', 'RAD']
VALID_FLUX_SYSTEMS = {'EF': 'EasyFlux', 'EP': 'EddyPro', 'DL': 'TERNflux'}
TURBULENT_FLUX_QUANTITIES = ['Fco2', 'Fe', 'Fh']
VALID_LOC_UNITS = ['cm', 'm']
VALID_SUFFIXES = {
    'Av': 'average', 'Sd': 'standard_deviation', 'Vr': 'variance',
    'Sum': 'sum', 'Ct': 'sum', 'QC': 'quality_control_flag'
    }
VARIANCE_CONVERSIONS = {
    'g^2/m^6': 'g/m^3',
    'umol/mol': 'umol/mol',
    'mg^2/m^6': 'mg/m^3',
    'degC^2': 'degC',
    'm^2/s^2': 'm/s',
    'mmol^2/m^6': 'mmol/m^3',
    'mmol/mol': 'mmol/mol',
    'K^2': 'K'
    }
_NAME_MAP = {'site_name': 'name', 'pfp_name': 'pfp_name'}

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN SITE METADATA MANAGER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class BaseMetaDataManager():
    """
    Class to read and interrogate variable metadata from site-specific
    config file
    """

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def __init__(self, df: pd.DataFrame, data_path: pathlib.Path):
        """
        

        Args:
            df (pd.DataFrame): DESCRIPTION.
            data_path (pathlib.Path): DESCRIPTION.

        Returns:
            None.

        """
    
        self._df = df
        self.data_path = data_path
        
    @property
    def variables(self) -> pd.DataFrame:
        return self._df  
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
    def get_variable_attributes(
            self, variable: str, source_field: str='pfp_name', 
            return_field: str=None
            ) -> pd.Series | str:
        """
        Get the attributes for a given variable.

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
        series = df.loc[variable]
        if return_field is None:
            return series
        return series[return_field]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variables_by_file(
            self, file: str, source_field: str='pfp_name'
            ) -> list:
        """
        Get the untranslated list of variable names.

        Args:
            file: name of file.

        Returns:
            list: list of untranslated variable names.

        """

        df = self._index_translator(use_index=source_field)
        return df[df.file==file].index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variables_by_quantity(
            self, quantity: str, source_field: str='pfp_name'
            ) -> list:
        """


        Args:
            quantity (TYPE): DESCRIPTION.

        Returns:
            list: DESCRIPTION.

        """

        df = self._index_translator(use_index=source_field)
        return df[df.quantity == quantity].index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    ### List ###
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_loggers(self) -> list:
        """
        List the loggers defined in the variable map.

        Returns:
            the list of loggers.

        """

        return self.variables.logger.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def list_tables(self) -> list:
        """
        List the tables defined in the variable map.

        Returns:
            the list of tables.

        """
        return self.variables.table.unique().tolist()
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
        the_list = self.variables.file.unique().tolist()
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

        return self.variables.index.tolist()
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
            self.variables.loc[
                self.variables.units!=
                self.variables.standard_units
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
            self.variables
            [self.variables.process=='Vr']
            .index.tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    ### Map ###
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
            self.variables
            .quantity
            .reset_index()
            .set_index(keys='quantity')
            .loc[TURBULENT_FLUX_QUANTITIES]
            .reset_index()
            .set_index(keys='pfp_name')
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
            self.variables[['logger', 'table']]
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
                data=self.variables.file.tolist(),
                index=self.variables.table.tolist()
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
    ### Translate ###
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
            self, table: str=None, source_field: str='site_name'
            ) -> dict:
        """
        Maps the translation between site names and pfp names for a specific
        table.

        Args:
            table (optional): name of table for which to fetch translations.
            Defaults to None.
            source_field (optional): the source field for the variable name
            (either 'pfp_name' or 'site_name'). Defaults to 'site_name'.

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
            (either 'pfp_name' or 'site_name'). Defaults to 'site_name'.

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
            (either 'pfp_name' or 'site_name'). Defaults to 'site_name'.

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
    def _get_target_field_from_source(self, source_field: str) -> str:
        """
        Gets the inverse of the source field.

        Args:
            source_field: field for which to retrieve inverse
            (if 'site_name' is source, 'pfp_name' is return, and vice versa).

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
            self.variables
            .reset_index()
            .set_index(keys=_NAME_MAP[use_index])
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class InputMetaDataManager(BaseMetaDataManager):
    
    #--------------------------------------------------------------------------
    def __init__(self, configs: dict, data_path: pathlib.Path):
        
        self.site_name = configs['site']
        global_attrs = configs.get("global_attrs", {})
        for field, value in global_attrs.items():
            setattr(self, field, value)
        # self.sonic_type = global_attrs.get('sonic_type')
        # self.irga_type = global_attrs.get('irga_type')
        # self.system_type = global_attrs.get('system_type')
        
        data_path = paths.get_local_stream_path(
            resource='raw_data', stream='flux_slow', site=configs['site']
            )
        
        # Build dataframe from metadata dict
        df = self._build_dataframe(configs=configs)
               
        # Initialize the base class with dataframe + path
        super().__init__(df, data_path)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_dataframe(self, configs):
        
        return (
            pd.DataFrame.from_dict(configs['variables'], orient='index')
            .rename_axis('pfp_name')
            )
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    @classmethod
    def from_yaml(cls, yml_path: pathlib.Path) -> "InputMetadataManager":
        """
        Factory to create an InputMetadataManager instance from a YAML file.
        """
        
        configs = read_yml(file=yml_path)
        
        # Do yml file / content validation
        # Validate YAML structure
        struct_validator = Config(**configs)
        
        # Validate standard names
        configs = validate_L1_config_names(configs=configs)
        
        # Validate the paths / variables
        data_path = paths.get_local_stream_path(
            resource='raw_data', stream='flux_slow', site=configs['site']
            )
        configs = validate_L1_config_paths(configs, data_path)

        # Get the raw flux file
        flux_file = get_raw_flux_file(configs=configs)

        # Add necessary stuff from validator
        configs['global_attrs'] = {
            'sonic_type': struct_validator.sonic_instrument,
            'irga_type': struct_validator.irga_instrument,
            'system_type': VALID_FLUX_SYSTEMS[struct_validator.flux_suffix],
            'flux_file': flux_file
            }

        # Construct instance
        return cls(configs, data_path)
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def list_variance_variables(self):
        
        return (
            self.variables
            [self.variables.statistic_type=='variance']
            .index.tolist()
            )
    #--------------------------------------------------------------------------
    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class OutputMetaDataManager(BaseMetaDataManager):
    
    
    def __init__(self, source: InputMetaDataManager):
        
        data_path = source.data_path
        df = source.variables.copy(deep=True)
        
        super().__init__(df=df, data_path=data_path)
        self._convert_variance_to_std()

    def _convert_variance_to_std(self):
        
        mask = self.variables['statistic_type'] == 'variance'
        self.variables.loc[mask, 'units'].apply(self._convert_units)
        self.variables.loc[mask, 'process'] = 'Sd'
        self.variables.loc[mask, "statistic_type"] = 'standard_deviation'
        self.variables.loc[mask, 'units'] = (
            self.variables.loc[mask, 'units'].apply(self._convert_units)
            )
        self.vr_to_sd_map = {
            variable: variable.replace('Vr', 'Sd') for variable in 
            self.variables[mask].index.tolist()
            }
        self.variables.rename(self.vr_to_sd_map, inplace=True)
        
    def _convert_units(self, variance_in):
        
        return VARIANCE_CONVERSIONS[variance_in]
    
    def _rename_var(self, variable_in):
        
        return variable_in.replace()
#------------------------------------------------------------------------------

###############################################################################
### END SITE METADATA MANAGER CLASS ###
###############################################################################


###############################################################################
### BEGIN CONFIG VALIDATOR CLASSES ###
###############################################################################


# ───────────────────────────────────────────────
# Per-variable configuration
# ───────────────────────────────────────────────
class VariableConfig(BaseModel):
    """
    Represents configuration metadata for a single data variable.

    Supports two mutually exclusive naming conventions:
    - (logger + table)
    - file

    Also supports:
    - diag_type for diagnostic variables (optional unless required by rules)
    - standard_name for custom user-defined variables
    """

    instrument: str
    statistic_type: str
    units: str
    height: str
    name: str

    # Two possible schemas:
    logger: Optional[str] = None
    table: Optional[str] = None
    file: Optional[str] = None

    diag_type: Optional[str] = None
    standard_name: Optional[str] = None   # indicates custom variable

    class Config:
        extra = "allow"   # allow unexpected user-added fields

    @field_validator("diag_type")
    def validate_diag_type_value(cls, v):
        """If present, diag_type must be valid_count or invalid_count."""
        if v is None:
            return v
        if v not in {"valid_count", "invalid_count"}:
            raise ValueError("diag_type must be one of: valid_count, invalid_count")
        return v

    @model_validator(mode="after")
    def validate_schema_choice(self):
        """Enforce exactly one schema: either file OR (logger + table)."""
        if self.file is not None:
            if self.logger is not None or self.table is not None:
                raise ValueError("Use either file OR logger+table, not both.")
        else:
            # file is None → require logger + table
            if self.logger is None or self.table is None:
                raise ValueError("Must define either file OR (logger AND table).")

        return self



# ───────────────────────────────────────────────
# Global configuration for a site
# ───────────────────────────────────────────────

class Config(BaseModel):
    """
    Represents full metadata configuration for a site.

    Performs validation on:
    - required fields in variables
    - diagnostic variable rules
    - sonic/IRGA instrument consistency
    - flux variable suffix consistency (EP/EF/DL)
    """

    site: str
    variables: Dict[str, VariableConfig]

    # class-level lists
    diag_prefixes: ClassVar[List[str]] = ["Diag_"]
    sonic_suffix: ClassVar[str] = "_SONIC"
    irga_suffix: ClassVar[str] = "_IRGA"
    flux_prefixes: ClassVar[List[str]] = ["Fco2", "Fe", "Fh", "Fm", "ustar"]

    # Site-wide attributes set by validators
    sonic_instrument: Optional[str] = None
    irga_instrument: Optional[str] = None
    diag_type: Optional[str] = None
    flux_suffix: Optional[str] = None

    # ───────────────────────────────────────────────
    # Validator: diagnostics must have diag_type
    # ───────────────────────────────────────────────
    @model_validator(mode="after")
    def enforce_diag_rules(self):
        diag_types = set()

        for var_name, cfg in self.variables.items():
            if any(var_name.startswith(prefix) for prefix in self.diag_prefixes):
                if cfg.diag_type is None:
                    raise ValueError(
                        f"Diagnostic variable '{var_name}' must define diag_type"
                    )
                diag_types.add(cfg.diag_type)

        if diag_types:
            if len(diag_types) > 1:
                raise ValueError(
                    f"Diagnostic variables have inconsistent diag_type values: "
                    f"{diag_types}. Must all be same."
                )
            self.diag_type = diag_types.pop()

        return self


    # ───────────────────────────────────────────────
    # Validator: sonic and IRGA instrument consistency
    # ───────────────────────────────────────────────
    @model_validator(mode="after")
    def enforce_instrument_consistency(self):
        sonic_instruments = set()
        irga_instruments = set()

        for var_name, cfg in self.variables.items():

            if var_name.endswith(self.sonic_suffix):
                sonic_instruments.add(cfg.instrument)

            if var_name.endswith(self.irga_suffix):
                irga_instruments.add(cfg.instrument)

        if len(sonic_instruments) > 1:
            raise ValueError(
                f"SONIC variables must use the same instrument; found {sonic_instruments}"
            )

        if len(irga_instruments) > 1:
            raise ValueError(
                f"IRGA variables must use the same instrument; found {irga_instruments}"
            )

        if sonic_instruments:
            self.sonic_instrument = sonic_instruments.pop()
        if irga_instruments:
            self.irga_instrument = irga_instruments.pop()

        return self


    # ───────────────────────────────────────────────
    # Validator: flux suffix consistency (EP/EF/DL)
    # ───────────────────────────────────────────────
    @model_validator(mode="after")
    def enforce_flux_suffix(self):
        suffixes_found = set()

        for var_name in self.variables:
            for prefix in self.flux_prefixes:
                if var_name.startswith(prefix):

                    # extract suffix after underscore
                    parts = var_name.split("_", 1)
                    if len(parts) != 2:
                        raise ValueError(
                            f"Flux variable '{var_name}' must end with _EP/_EF/_DL"
                        )
                    suffix = parts[1]

                    if suffix not in set(VALID_FLUX_SYSTEMS.keys()):
                        raise ValueError(
                            f"Flux variable '{var_name}' has invalid suffix '{suffix}'. "
                            "Must be one of: EP, EF, DL."
                        )

                    suffixes_found.add(suffix)

        if suffixes_found:
            if len(suffixes_found) > 1:
                raise ValueError(
                    f"Flux variables must share the same suffix (EP/EF/DL). "
                    f"Found: {suffixes_found}"
                )
            self.flux_suffix = suffixes_found.pop()

        return self

    # ----------------------------------------------------------------------
    # 4. Compute derived attributes (after all validation)
    # ----------------------------------------------------------------------
    def model_post_init(self, _ctx):
        sonic = {
            cfg.instrument
            for name, cfg in self.variables.items()
            if self.sonic_suffix in name
        }
        irga = {
            cfg.instrument
            for name, cfg in self.variables.items()
            if self.irga_suffix in name
        }

        self.sonic_instrument = next(iter(sonic), None)
        self.irga_instrument = next(iter(irga), None)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def validate_L1_config_structure(file):
           
    # Check that all fields are correctly structured in config file 
    # (will fail if not - let tasks module catch and log it)
    return Config(**read_yml(file=file))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def validate_L1_config_names(configs: dict) -> dict:
    
    parser = PFPNameParser()
    for variable, attrs in configs['variables'].items():
        if 'long_name' in attrs.keys():
            attrs.update(
                {'standard_units': attrs['units'], 'is_custom': True}
                )
        else:
            attrs.update(parser.parse_variable_name(variable_name=variable))
            attrs.update({'is_custom': False})
    return configs            
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def validate_L1_config_paths(
        configs: dict, data_path: str | pathlib.Path
        ) -> dict:
    
    
    # Create file names if they don't exist
    data_path = pathlib.Path(data_path)
    temp_df = pd.DataFrame.from_dict(configs['variables'], orient='index')
    if not 'file' in temp_df.columns:
        temp_df['file'] = (
            f'{configs["site"]}_' +
            temp_df[['logger', 'table']].agg('_'.join, axis=1) + 
            '.dat'
            )
    
    # Validate the file paths and the variable allocations to those files
    groups = temp_df.groupby(temp_df.file)
    for this_tuple in groups:
        file_path = data_path / this_tuple[0]
        if not file_path.exists():
            raise FileNotFoundError(f'File path {file_path} does not exist!')
        check_vars = this_tuple[1].name.tolist()
        header_df = io.get_header_df(file=file_path)
        for var in check_vars:
            if not var in header_df.index:
                raise KeyError(
                    f'Variable {var} not found in file {this_tuple[0]}'
                    )
    
    # Update the existing variable set to include file
    configs['variables'] = temp_df.to_dict(orient='index')
    return configs
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_raw_flux_file(configs):
    
    var_list = []
    for variable, attrs in configs['variables'].items():
        if attrs['quantity'] in TURBULENT_FLUX_QUANTITIES:
            var_list.append(attrs['file'])
    if all([var==var_list[0] for var in var_list]):
        return var_list[0]
    raise RuntimeError(
        'Turbulent flux variables must be contained in a single file source'
        )
#------------------------------------------------------------------------------



# #------------------------------------------------------------------------------
# def check_L1_config_file(file: str, data_path: str | pathlib.Path):
    
#     data_path = pathlib.Path(data_path)
    
#     # Get the structural yml validator
#     validator = validate_L1_config_structure(file=file, return_validator=True)

#     # Make df from validator
#     configs_df = pd.DataFrame(
#         [v.model_dump() for v in validator.variables.values()],
#         index=validator.variables.keys()
#         )
    
#     # Add columns
#     try:
#         configs_df['is_custom'] = ~pd.isnull(configs_df['long_name'])
#     except KeyError:
#         configs_df['is_custom'] = False
#     for field in ['file', 'long_name']:
#         if not field in configs_df.columns:
#             configs_df['file'] = None
    
#     # Iterate over variables and check that the variable names conform to PFP naming rules
#     parser = PFPNameParser()
#     props_list = []   
#     for variable in configs_df.index:
        
#         # If a custom variable, bypass the parser and create a minimal set of properties
#         # Otherwise, run the parser and pop the long name into the existing long_name col
#         if configs_df.loc[variable, 'is_custom']:
#             temp_dict = {'standard_units': configs_df.loc[variable, 'units']}
#         else:
#             temp_dict = parser.parse_variable_name(variable_name=variable)
#             configs_df.loc[variable, 'long_name'] = temp_dict.pop('long_name')
#         props_list.append(temp_dict)
        
#         # If there is not an existing file name (usually the case), make it
#         if configs_df.loc[variable, 'file'] is None:
#             logger, table = configs_df.loc[variable, ['logger', 'table']]
#             configs_df.loc[variable, 'file'] = (
#                 '_'.join([validator.site, logger, table]) + '.dat'
#                 )
                   
#     # Make a dataframe from the properties unpacked from the variable name parser
#     props_df = (
#         pd.DataFrame(props_list)
#         .set_index(configs_df.index)
#         )
    
#     # Validate the file paths and the variable allocations to those files
#     groups = configs_df.groupby(configs_df.file)
#     for this_tuple in groups:
#         file_path = data_path / this_tuple[0]
#         if not file_path.exists():
#             raise FileNotFoundError(f'File path {file_path} does not exist!')
#         check_vars = this_tuple[1].name.tolist()
#         header_df = io.get_header_df(file=file_path)
#         for var in check_vars:
#             if not var in header_df.index:
#                 raise KeyError(
#                     f'Variable {var} not found in file {this_tuple[0]}'
#                     )

#     # Concatenate the variable dataframe
#     variables_df = (
#         pd.concat([configs_df, props_df], axis=1)
#         .fillna('')
#         .rename_axis('pfp_name')
#         )

#     # Check that all the data is contained in a single file
#     flux_file = (
#         variables_df[variables_df.quantity.isin(TURBULENT_FLUX_QUANTITIES)]
#         .file
#         .unique()
#         )
#     if len(flux_file) > 1:
#         raise RuntimeError(
#             'Turbulent flux variables must be contained in a single file source'
#             )
#     flux_file = flux_file.item()    
        
#     # Concatenate and return
#     return {
#         'site': validator.site,
#         'system_type': VALID_FLUX_SYSTEMS[validator.flux_suffix],
#         'variables': variables_df,
#         'sonic_type': validator.sonic_instrument,
#         'irga_type': validator.irga_instrument,
#         'flux_file': flux_file
#         }
# #------------------------------------------------------------------------------  

#------------------------------------------------------------------------------
def read_yml(file):
    
    with open(file) as f:
        return yaml.safe_load(stream=f)
#------------------------------------------------------------------------------


###############################################################################
### END CONFIG VALIDATOR CLASSES ###
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

    2) If there is a process identifier, it must occur as the last substring.
    If it is 'Vr', this must become part of the unique variable identifier,
    because variances necessarily have different units.

    3) A system type identifier can be added but must immediately follow the
    unique variable identifier.

    4) Location and replicate identifiers can be added, but must be in order
    of vertical identifier (which must have units of cm or m),
    horizontal identifier (single alpha character only) and replicate
    identifiers (numeric characters only). They do not all need to be present.
    They must NOT be separated by underscores.

    Second component can be either a unique device identifier (listed under
    VALID_INSTRUMENTS in this module) or a location identifier. The former are
    variables that are either unique to that device or that would be ambiguous
    in the absence of a device identifier (e.g. AH versus AH_IRGA).


    """

    #--------------------------------------------------------------------------
    def __init__(self):
        """
        Load the naming info from the `std_names` yml.

        Returns:
            None.

        """

        data = read_yml(
            pathlib.Path(__file__).parents[1] / 'configs' / 'pfp_std_names.yml'
            )
        self.variables = (
            pd.DataFrame.from_dict(data=data, orient='index')
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

        # Init result dict
        rslt_dict = {
            'quantity': None,
            'instrument_type': None,
            'vertical_location': None,
            'horizontal_location':None,
            'replicate': None,
            'process': None,
            'system_type': None
            }

        # Init error list
        errors = []

        # Get string elements
        elems = variable_name.split('_')

        # Find quantity and instrument (if valid);
        # We can't proceed without a valid quantity, so let this fail without
        # catching
        rslt_dict.update(self._check_str_is_quantity(parse_list=elems))

        # Iterate over the checking functions, progressively removing elements
        # and updating dicts
        for func in [
            self._check_str_is_process,
            self._check_str_is_system_type,
            self._check_str_is_vertical_location,
            self._check_str_is_horizontal_location,
            self._check_str_is_replicate_num
            ]:

            try:
                rslt_dict.update(func(parse_list=elems))
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
        if rslt_dict['process'] == 'Ct':
            edit_count_info(info=rslt_dict)
        if rslt_dict['process'] == 'QC':
            edit_QC_info(info=rslt_dict)

        # Return results
        return rslt_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_quantity(self, parse_list: list) -> dict:
        """
        Check the list of elements for quantity substrings.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            TypeError: raised if a valid quantity identifier not found.

        Returns:
            the quantity (and instrument, if valid).

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
            raise TypeError(
                f'{quantity} is not a valid quantity identifier!'
                )

        return {
            'quantity': quantity,
            'instrument_type': instrument,
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_process(self, parse_list: list) -> dict:
        """
        Check the list of elements for a valid process as its final element.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            TypeError: raised if a valid process identifier not found.

        Returns:
            the process.

        """

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
        """
        Check the list of elements for a system type suffix.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            TypeError: raised if a valid quantity identifier not found.

        Returns:
            the system type.

        """

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
    def _check_str_is_vertical_location(self, parse_list: str) -> dict:
        """
        Check the list of elements for vertical location.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            TypeError: raised if a valid vertical location identifier not found.

        Returns:
            the vertical location.

        """

        if len(parse_list) == 0:
            return {}

        parse_str = parse_list[0]

        for units in VALID_LOC_UNITS:

            # If no location units, length will be one - skip if so
            sub_list = parse_str.split(units)
            if len(sub_list) == 1:
                error = (
                    'No recognised height / depth units in location '
                    'identifier!'
                    )
                continue

            # Strip any '' elements out of the list (split adds '' to list
            # when the split characters are the last elements)
            if sub_list[1] == '':
                sub_list = sub_list[:1]

            # Parse the first list element to check for depth-integrated sensor
            # (e.g. vertical soil moisture `Sws_0-30cm`)
            split_list = sub_list[0].split('-')
            if len(split_list) > 2:
                error = (
                    'A maximum of two height / depth identifiers is allowed!'
                    )
            try:
                [float(elem) for elem in split_list]
            except ValueError:
                error = (
                    'Characters preceding height / depth units must be '
                    'numeric, or contain numerals separated by single "-"!'
                    )
                continue

            # If we get this far, we send the remaining substring (if any)
            # back to the parse list to be evaluated as a potential replicate,
            # and return the vertical location
            vertical_location = sub_list[0] + units
            if parse_list[0] == vertical_location:
                parse_list.remove(vertical_location)
            else:
                parse_list[0] = parse_list[0].replace(vertical_location, '')
            return {'vertical_location': vertical_location}

        # Raise error if not recognised!
        raise TypeError(
            error + f' Passed substring "{parse_str}" does not conform!'
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_horizontal_location(self, parse_list: str) -> dict:
        """
        Check the list of elements for horizontal location.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            TypeError: raised if a valid horizontal identifier not found.

        Returns:
            the horizontal location.

        """

        if len(parse_list) == 0:
            return {}
        horizontal_location = parse_list[0][0]
        if not horizontal_location.isalpha():
            raise TypeError(f'{horizontal_location} is not an alpha character!')
        if parse_list[0] == horizontal_location:
            parse_list.remove(horizontal_location)
        else:
            parse_list[0] = parse_list[0].replace(horizontal_location, '')
        return {'horizontal_location': horizontal_location}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_str_is_replicate_num(self, parse_list: str) -> dict:
        """
        Check the list of elements for replicate number.

        Args:
            parse_list: the list of substring elements to parse.

        Raises:
            TypeError: raised if a valid replicate identifier not found.

        Returns:
            the replicate number.

        """

        if len(parse_list) == 0:
            return {}

        is_rep_num = parse_list[0]
        if not is_rep_num.isdigit():
            raise TypeError('Replicate number must be an integer!')
        parse_list.remove(is_rep_num)
        return {'replicate': is_rep_num}
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
        'm/s': 'm^2/s^2',
        'mmol/m^3': 'mmol^2/m^6',
        'mmol/mol': 'mmol/mol'
        }

    if not to_variance:
        ref_dict = {value: key for key, value in ref_dict.items()}
    return ref_dict[units]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def edit_count_info(info: dict) -> None:
    """
    Edit muteable dict of variable properties

    Args:
        info (dict): DESCRIPTION.

    Returns:
        None: DESCRIPTION.

    """

    info['plausible_min'] = 0
    info['plausible_max'] = None
    info['standard_units'] = '1'
    info['long_name'] = 'Number of samples of ' + info['long_name']
    info['standard_name'] = None
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def edit_QC_info(info: dict) -> None:
    """
    Edit muteable dict of variable properties

    Args:
        info (dict): DESCRIPTION.

    Returns:
        None: DESCRIPTION.

    """

    info['plausible_min'] = 0
    info['plausible_max'] = None
    info['standard_units'] = '1'
    info['long_name'] = 'QC flag value of ' + info['long_name']
    info['standard_name'] = None
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
