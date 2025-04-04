# -*- coding: utf-8 -*-
"""
Created on Thu May 16 16:25:29 2024

@author: imchugh

This module is used to read data from an existing set of netcdf files and allow
analysis and write back toan output file.

Module classes:
    - NCtoTOA5Reader: reads data and metadata from existing netcdf files and
    makes data and metadata available as pandas dataframes.
    - NCtoTOA5Constructor: compiles and parses data from raw form to a
    formatted dataset that can be written to an output TOA5 file.

Todo:
    - the reader currently generates TOA5 headers, but that should ideally be \
    done by the TOA5 constructor;
    - the concatenation of all compatible L1 netcdf files should ideally take
    place within the reader.

"""

#------------------------------------------------------------------------------
### IMPORTS ###
#------------------------------------------------------------------------------
# Standard imports #
import logging
import pandas as pd
import pathlib
import xarray as xr

# Custom imports #
from data_constructors import convert_calc_filter as ccf
from file_handling import file_io as io
from managers import metadata as md
from managers import paths
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### INITIALISATIONS AND CONSTANTS ###
#------------------------------------------------------------------------------
STATISTIC_ALIASES = {
    'average': 'Avg', 'variance': 'Vr', 'standard_deviation': 'Sd',
    'sum': 'Tot'
    }
VARIABLE_ALIASES = {'Wd_SONIC_Av': 'Wd', 'Ws_SONIC_Av': 'Ws'}
TRUNCATE_VARIABLES = ['Fco2', 'Fe', 'Fh']
ADD_VARIABLES = ['AH', 'RH', 'CO2_IRGA', 'Td', 'VPD']
FLUX_FILE_VAR_IND = 'Uz_SONIC_Av'
logger = logging.getLogger(__name__)
#------------------------------------------------------------------------------



###############################################################################
### BEGIN NETCDF READER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class NCtoTOA5Reader():
    """
    Class to allow conversion of data from NetCDF format back to TOA5-like
    data and headers. It allows accessing of the variable and global attributes
    of the input netcdf file.
    """

    #--------------------------------------------------------------------------
    def __init__(self, nc_file: pathlib.Path | str) -> None:
        """
        Open the NetCDF file as xarray dataset, and set QC flags and coordinate
        reference system variables as labels to drop.

        Args:
            nc_file: Absolute path to NetCDF file.

        Returns:
            None.

        """

        self.ds = xr.open_dataset(nc_file)
        self.labels_to_drop = (
            ['crs'] + [x for x in self.ds if 'QCFlag' in x]
            )
        self.labels_to_keep = [
            var for var in self.ds if not var in self.labels_to_drop
            ]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_dataframe(self) -> pd.core.frame.DataFrame:
        """
        Strip back the dataset to the minimum required for the dataframe.

        Returns:
            dataframe.

        """

        return (
            self.ds
            .to_dataframe()
            .droplevel(['latitude', 'longitude'])
            .drop(self.labels_to_drop, axis=1)
            .rename_axis('DATETIME')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_globals_attrs(self) -> dict:

        return self.ds.attrs()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attrs(self, variable: str) -> dict:

        return self.ds.variables[variable].attrs
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_headers(self) -> pd.core.frame.DataFrame:
        """
        Create the header dataframe required for the TOA5 conversion.

        Returns:
            dataframe.

        """

        return pd.DataFrame(
            data = [
                {
                    'units': self.ds[var].attrs['units'],
                    'sampling':
                        STATISTIC_ALIASES[self.ds[var].attrs['statistic_type']]
                    }
                for var in self.labels_to_keep
                ],
            index=self.labels_to_keep
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END NETCDF READER CLASS ###
###############################################################################



###############################################################################
### BEGIN VISUALISATION DATA BUILDER CLASS ###
###############################################################################

class NCtoTOA5Constructor():
    """
    Read data from L1 netcdf file and reformat data for output to
    visualisation TOA5 file.
    """

    #--------------------------------------------------------------------------
    def __init__(self, site: str) -> None:
        """
        Read data and reformat ready for output.

        Args:
            site: namer of site.

        Returns:
            None.

        """

        self.site = site
        self._init_all()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _init_all(self):
        """
        Get the NCReader and requisite components (data, attributes) and build
        translation mapper.

        Returns:
            None.

        """

        self.input_path = paths.get_local_stream_path(
            resource='homogenised_data', stream='nc', subdirs=[self.site]
            )
        self.output_path = paths.get_local_stream_path(
            resource='homogenised_data', stream='TOA5'
            )
        self.files = sorted([file.name for file in self.input_path.glob('*.nc')])
        reader = NCtoTOA5Reader(nc_file=self.input_path / self.files[-1])
        self.translation_dict = self._build_translation_dict(reader=reader)
        self.drop_list = self._build_drop_list(reader=reader)
        self.headers = self._build_headers(reader=reader)
        self.var_attrs = self._build_attrs(reader=reader)
        self.data, self.used_files = self._build_data(reader=reader)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_drop_list(self, reader: NCtoTOA5Reader) -> list:
        """
        Subset the data to requisite variables

        Args:
            reader: netcdf reader class.

        Returns:
            vars_to_drop: list of variables to remove.

        """

        soil_keys = {
            'soil_heat_flux': 'Fg',
            'soil_temperature': 'Ts',
            'soil_moisture': 'Sws'
            }
        vars_to_drop = [var for var in reader.labels_to_keep if 'Sd' in var]
        soil_to_keep = (
            paths.get_internal_configs(config_name='soil_variables')
            [self.site]
            )
        for quantity, keep_variables in soil_to_keep.items():
            quantity_str = soil_keys[quantity] + '_'
            available_variables = [
                var for var in reader.labels_to_keep if quantity_str in var
                ]
            vars_to_drop += [
                var for var in available_variables if not var in keep_variables
                ]
        return vars_to_drop
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_translation_dict(self, reader: NCtoTOA5Reader) -> dict:
        """
        Map the existing to translated variable names.

        Args:
            reader: netcdf reader class.

        Returns:
            dictionary mapping.

        """

        # Create rename dictionary
        return (
            self._translate_fluxes(reader=reader) |
            self._translate_averages(reader=reader) |
            self._translate_met_variables(reader=reader) |
            self._translate_co2_variables(reader=reader) |
            VARIABLE_ALIASES
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _translate_fluxes(self, reader: NCtoTOA5Reader) -> dict:
        """
        Remove system-based suffixes from raw logger fluxes.

        Args:
            reader: netcdf reader class.

        Returns:
            dictionary mapping.

        """

        var_list = []
        for flux_var in md.TURBULENT_FLUX_QUANTITIES:
            for file_var in reader.get_dataframe().columns:
                if flux_var in file_var:
                    split_list = file_var.split('_')
                    if len(split_list) == 2:
                        if split_list[-1] in md.VALID_FLUX_SYSTEMS.keys():
                            var_list.append(file_var)
        return dict(zip(var_list, md.TURBULENT_FLUX_QUANTITIES))
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _translate_met_variables(self, reader: NCtoTOA5Reader) -> dict:
        """
        Find nearest T and RH sensor to flux height and create translation
        alias.

        Args:
            reader: netcdf reader class.

        Returns:
            rename_dict: dictionary mapping.

        """


        int_extractor = lambda height: float(height.replace('m', ''))
        target_height = int_extractor(
            reader.get_variable_attrs(variable=FLUX_FILE_VAR_IND)['height']
            )
        var_dict = {
            abs(
                target_height -
                int_extractor(reader.get_variable_attrs(variable=var)['height'])
                ):
                var for var in reader.labels_to_keep if 'Ta' in var
            }
        var = var_dict[min(var_dict.keys())]
        var_suffix = var.replace(var.split('_')[0], '')
        rename_dict = {var_dict[min(var_dict.keys())]: 'Ta'}
        for quantity in ['RH', 'AH']:
            test_for_var = quantity + var_suffix
            if test_for_var in reader.labels_to_keep:
                rename_dict[test_for_var] = quantity
        return rename_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _translate_averages(self, reader: NCtoTOA5Reader) -> dict:
        """
        Translate averages by dropping suffix.

        Args:
            reader: netcdf reader class.

        Returns:
            dictionary mapping.

        """

        return {
            var: var.replace('_Av', '') for var in reader.labels_to_keep
            if '_Av' in var
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _translate_co2_variables(self, reader: NCtoTOA5Reader) -> dict:
        """
        Sort out mess with CO2 having two distinct identities for same
        variable name.

        Args:
            reader: netcdf reader class.

        Returns:
            dictionary mapping.

        """

        co2_units = reader.get_variable_attrs(variable='CO2_IRGA_Av')['units']
        if co2_units == 'mg/m^3':
            return {'CO2_IRGA_Av': 'CO2c_IRGA'}
        if co2_units == 'umol/mol':
            return {'CO2_IRGA_Av': 'CO2_IRGA'}
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_attrs(self, reader: NCtoTOA5Reader) -> pd.DataFrame:
        """
        Read the variable attributes from the reader.

        Args:
            reader: netcdf reader class.

        Returns:
            attributes df.

        """

        # Parse the netcdf reader and collect the attributes
        rslt = {}
        parser = md.PFPNameParser()
        for variable in reader.get_headers().index:
            parser_name = variable
            if 'CO2_IRGA' in variable:
                if reader.get_variable_attrs(variable)['units'] == 'mg/m^3':
                    parser_name = variable.replace('CO2', 'CO2c')
            rslt[variable] = (
                reader.get_variable_attrs(variable=variable) |
                parser.parse_variable_name(variable_name=parser_name)
                )

        # Generate dataframe
        return (
            pd.DataFrame(rslt)
            .T
            .rename(self.translation_dict)
            .drop(self.drop_list)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_headers(self, reader: NCtoTOA5Reader) -> pd.DataFrame:
        """
        Reconstruct the TOA5 headers from the nc reader.

        Args:
            reader: netcdf reader class.

        Returns:
            headers df.

        """

        return (
            reader
            .get_headers()
            .rename(self.translation_dict)
            .drop(self.drop_list)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_data(self, reader) -> (pd.DataFrame, list):
        """
        Reconstruct the TOA5 headers from the nc reader.

        Args:
            reader: netcdf reader class.

        Returns:
            data: headers df.
            concat_list: the files that were safely concatenated.

        """

        # Initialise lists
        data_list  = [reader.get_dataframe()]
        concat_list = [self.files[-1]]

        # Iterate through files and check which can be concatenated
        master_header = reader.get_headers()
        for file in self.files[:-1]:
            reader = NCtoTOA5Reader(nc_file=self.input_path / file)
            header = reader.get_headers()
            if len(header) == len(master_header):
                if all(header.index == master_header.index):
                    concat_list.append(file)
                    data_list.append(reader.get_dataframe())

        # Concatenate data
        data = (
            pd.concat(data_list)
            [master_header.index]
            .sort_index()
            .rename(self.translation_dict, axis=1)
            .drop(self.drop_list, axis=1)
            )

        return data, concat_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def parse_data(self) -> pd.DataFrame:
        """
        Convert units, apply limits and add missing variables.

        Returns:
            parsed dataframe.

        """

        return (
            self.data.copy()
            .pipe(self._apply_limits)
            .pipe(self._add_missing_variables)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variables_for_conversion(self) -> list:
        """
        Find the variables that need unit conversion.

        Returns:
            the list of variables.

        """

        return (
            self.var_attrs.loc[
                self.var_attrs.units!=self.var_attrs.standard_units
                ]
            .index
            .tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _apply_limits(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply range limits.

        Args:
            df: data to filter.

        Returns:
            None.

        """

        for variable in data.columns:
            attrs = self.var_attrs.loc[variable]
            data[variable] = ccf.filter_range(
                series=data[variable],
                max_val=attrs.plausible_max,
                min_val=attrs.plausible_min
                )
        return data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_missing_variables(self) -> list:
        """
        Generate the list of required variables that are not present in the
        file and need to be built.

        Returns:
            list of variables.

        """

        return [
            x for x in ADD_VARIABLES
            if not x in self.var_attrs.quantity.tolist()
            ]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _add_missing_variables(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Add missing variables to data.

        Args:
            data: dataframe without variables.

        Raises:
            ValueError: raised if the conversion function is missing.

        Returns:
            data: the dataframe with added variables.

        """

        for var in self.get_missing_variables():
            try:
                rslt = ccf.get_function(variable=var, with_params=True)
                args_dict = {
                    parameter: data[parameter] for parameter in
                    rslt[1]
                    }
                data[var] = rslt[0](**args_dict)
            except KeyError as e:
                raise ValueError(
                    f'No conversion function for variable {var}'
                    ) from e
        return data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def parse_headers(self) -> pd.DataFrame:
        """
        Add the converted units and missing variables.

        Returns:
            the header dataframe with amended units and added variables.

        """

        # Alter units that need to be converted
        conversion_vars = self.get_variables_for_conversion()
        altered_headers = self.headers.copy()
        for var in conversion_vars:
            altered_headers.loc[var, 'units'] = (
                self.var_attrs.loc[var, 'standard_units']
                )

        # Add variables (and units) that are missing
        missing_vars = self.get_missing_variables()
        var_attrs = paths.get_internal_configs('pfp_std_names')
        units = [var_attrs[var]['standard_units'] for var in missing_vars]
        missing_headers = pd.DataFrame(
            data=units,
            index=missing_vars,
            columns=['units']
            )

        # Concatenate and return
        return (
            pd.concat([altered_headers, missing_headers])
            .fillna('')
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_TOA5(
            self, output_path: pathlib.Path | str=None, overwrite=True
            ) -> None:
        """
        Write out the data to a TOA5-formatted file.

        Args:
            file_path: the output path for the file.

        Returns:
            None.

        """

        # Check paths
        if output_path is None:
            output_path = self.output_path / f'{self.site}_merged_std.dat'
        else:
            output_path = pathlib.Path(output_path)
        if output_path.exists():
            if not overwrite:
                raise FileExistsError('File already created for year {year}!')

        headers = io.reformat_headers(
            headers=self.parse_headers(),
            output_format='TOA5'
            )
        data = io.reformat_data(
            data=self.parse_data(),
            output_format='TOA5'
            )
        info = dict(zip(
            io.INFO_FIELD_NAMES,
            io.FILE_CONFIGS['TOA5']['dummy_info'][:-1] + ['merged']
            ))
        io.write_data_to_file(
            headers=headers,
            data=data,
            info=info,
            abs_file_path=output_path,
            output_format='TOA5'
            )

    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END VISUALISATION DATA BUILDER CLASS ###
###############################################################################
