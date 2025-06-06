# -*- coding: utf-8 -*-
"""
Created on Thu May 16 16:25:29 2024

@author: imchugh

This module is used to read data from an existing set of netcdf files and
write back to a TOA5 output file for visualisation.

"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import logging
import pandas as pd
import pathlib
import xarray as xr

#------------------------------------------------------------------------------

from data_constructors import convert_calc_filter as ccf
from file_handling import file_io as io
from managers import metadata as md
from managers import paths

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

STATISTIC_ALIASES = {
    'average': 'Avg', 'variance': 'Vr', 'standard_deviation': 'Sd',
    'sum': 'Tot'
    }
ADD_VARIABLES = ['AH', 'RH', 'CO2_IRGA', 'Td', 'VPD']
FLUX_FILE_VAR_IND = 'Uz_SONIC_Av'
logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def construct_visualisation_TOA5(site: str, n_files=None) -> None:
    """
    Build a TOA5 file from the netcdf L1 files.

    Args:
        site: name of site.

    Returns:
        None.

    """

    # Initialise paths and files
    input_path = paths.get_local_stream_path(
        resource='homogenised_data', stream='nc', subdirs=[site]
        )
    output_path = (
        paths.get_local_stream_path(
            resource='homogenised_data', stream='TOA5'
            ) /
        f'{site}_merged_std.dat'
        )
    files = sorted(file for file in input_path.glob('*.nc'))
    if n_files is not None:
        files = files[-n_files:]

    # Lazy load all years into xarray dataset, then:
        # drop extraneous variables
        # rename to standard output naming
        # apply range limits
        # add requisite variables that are missing
    logger.info('Reading .nc files...')
    ds = (
        xr.open_mfdataset(paths=files, chunks={'time': 1000})
        .pipe(_drop_extraneous_variables)
        .pipe(_rename_variables)
        .pipe(_apply_limits)
        .pipe(_add_missing_variables)
        )
    logger.info('Done!')

    logger.info('Formatting data and headers...')

    # Compute dataframe and drop static dimensions (lat, long)
    df = (
        ds.to_dataframe()
        .droplevel(['latitude', 'longitude'])
        )

    # Generate output headers
    headers = _build_headers(ds=ds)

    logger.info('Done!')

    logger.info('Writing to TOA5 file...')

    # Write to output file
    _write_to_TOA5(
        data=df,
        headers=headers,
        output_path=output_path
        )

    logger.info('Done!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _drop_extraneous_variables(ds: xr.Dataset) -> xr.Dataset:

    """
    Subset the data to requisite variables.

    Args:
        ds: the xarray dataset containing the data.

    Returns:
        dataset with extraneous variables removed.

    """

    logger.info('    - Removing extraneous variables...')

    # Drop QC vars, standard deviations, coordinate reference system
    ds = ds.drop(
        ['crs'] +
        [var for var in ds.variables if var.endswith('QCFlag')] +
        [var for var in ds.variables if var.endswith('Sd')]
        )

    # Create drop list
    drop_list = []

    # Drop extraneous soil variables
    soil_keys = {
        'soil_heat_flux': 'Fg',
        'soil_temperature': 'Ts',
        'soil_moisture': 'Sws'
        }

    try:
        vars_to_keep = (
            paths.get_internal_configs(config_name='soil_variables')
            [ds.attrs['site_name']]
            )
    except KeyError:
        vars_to_keep = {}

    for quantity, keep_variables in vars_to_keep.items():
        quantity_str = soil_keys[quantity] + '_'
        available_variables = [
            var for var in ds.variables if var.startswith(quantity_str)
            ]
        drop_list += [
            var for var in available_variables if not var in keep_variables
            ]

    # Drop extraneous temp / RH / AH variables
    int_extractor = lambda height: float(height.replace('m', ''))
    target_height = int_extractor(ds[FLUX_FILE_VAR_IND].attrs['height'])
    temp_vars = [var for var in ds.variables if var.startswith('Ta')]
    var_dict = {
        var: abs(target_height - int_extractor(ds[var].attrs['height']))
        for var in temp_vars
        }
    keep_var = min(var_dict, key=var_dict.get)
    temp_vars.remove(keep_var)
    drop_list += temp_vars
    for quantity in ['RH', 'AH']:
        keep_this_var = keep_var.replace('Ta', quantity)
        for var in ds.variables:
            if var == keep_this_var:
                continue
            if 'IRGA' in var:
                continue
            if var.startswith(quantity):
                drop_list.append(var)

    # for quantity in ['RH', 'AH']:
    #     expected_vars = [var.replace('Ta', quantity) for var in temp_vars]
    #     for var in expected_vars:
    #         if var in ds.variables:
    #             drop_list.append(var)

    # Remove all extraneous variables
    return ds.drop(drop_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _rename_variables(ds: xr.Dataset) -> xr.Dataset:
    """
    Map the existing to translated variable names.

    Args:
        ds: the xarray dataset containing the data.

    Returns:
        dataset with renamed variables.

    """

    logger.info('    - Renaming variables...')

    # Remove system type identifier suffixes from fluxes
    rslt = {}
    for flux_var in md.TURBULENT_FLUX_QUANTITIES:
        for var in ds.variables:
            elems = var.split('_')
            if elems[0] == flux_var and len(elems) == 2:
                rslt.update({var: flux_var})
    ds = ds.rename(rslt)


    # Remove average suffixes from ALL variables
    ds = ds.rename(
        {
            var: var.replace('_Av', '') for var in ds.variables
            if var.endswith('_Av')
            }
        )

    # Remove met sensor vertical identifier
    rslt = {}
    for met_var in ['Ta', 'RH', 'AH']:
        rslt.update(
            {
                var: met_var for var in ds.variables
                if var.startswith(met_var)
                and not 'IRGA' in var
                }
            )
    ds = ds.rename(rslt)

    # Rename wind data
    ds = ds.rename({'Wd_SONIC': 'Wd', 'Ws_SONIC': 'Ws'})

    # Get right name for CO2 units
    co2_units = ds.variables['CO2_IRGA'].attrs['units']
    if co2_units == 'mg/m^3':
        rslt = {'CO2_IRGA': 'CO2c_IRGA'}
    ds = ds.rename(rslt)

    # Check for multiple rain replicates, use the first...
    precip_list = [var for var in ds.variables if 'Precip' in var]
    precip_var = precip_list[0]
    if not precip_var == 'Precip':
        ds = ds.rename({precip_var: 'Precip'})

    return ds
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _apply_limits(ds: xr.Dataset) -> xr.Dataset:
    """
    Apply range limits.

    Args:
        ds: the xarray dataset containing the data.

    Returns:
        dataset with limits applied.

    """

    logger.info('    - Applying variable range limits...')
    parser = md.PFPNameParser()
    variables = list(ds.variables)
    for variable in ['latitude','longitude']:
        variables.remove(variable)
    for variable in variables:
        try:
            attrs = parser.parse_variable_name(variable_name=variable)
        except TypeError:
            logger.error(
                f'      Variable {variable} naming does not pass '
                'standardisation tests!'
                )
            attrs = {'plausible_min': None, 'plausible_max': None}
        ds[variable] = ccf.filter_range(
            series=ds[variable],
            max_val=attrs['plausible_max'],
            min_val=attrs['plausible_min']
            )
    return ds
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _add_missing_variables(ds: xr.Dataset) -> xr.Dataset:
    """
    Add missing variables to data.

    Args:
        ds: the xarray dataset containing the data.

    Raises:
        ValueError: raised if the conversion function is missing.

    Returns:
        dataset with added variables.

    """

    logger.info('    - Calculating and adding missing variables...')

    ATTRS_LIST = ['plausible_max', 'plausible_min', 'units']
    GENERIC_DICT = {'statistic_type': 'average'}
    parser = md.PFPNameParser()
    for var in ADD_VARIABLES:
        if not var in ds.variables:
            try:
                rslt = ccf.get_function(variable=var, with_params=True)
                args_dict = {
                    parameter: ds[parameter] for parameter in
                    rslt[1]
                    }
                ds[var] = rslt[0](**args_dict)
                rslt = parser.parse_variable_name(variable_name=var)
                rslt['units'] = rslt.pop('standard_units')
                ds[var].attrs = (
                    {
                        key: value for key, value in rslt.items() if key in
                        ATTRS_LIST
                        } |
                    GENERIC_DICT
                    )
            except KeyError as e:
                raise ValueError(
                    f'No conversion function for variable {var}'
                    ) from e
    return ds
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _build_headers(ds: xr.Dataset) -> pd.DataFrame:
    """
    Reconstruct the TOA5 headers from the dataset.

    Args:
        ds: the dataset to mine for the TOA5 header info.

    Returns:
        headers dataframe.

    """

    remove_vars = ['time', 'latitude', 'longitude']
    var_list = list(ds.variables)
    for var in remove_vars:
        var_list.remove(var)
    return pd.DataFrame(
        data = [
            {
                'units': ds[var].attrs['units'],
                'sampling': STATISTIC_ALIASES[
                    ds[var].attrs['statistic_type']
                    ]
                }
            for var in var_list
            ],
        index=var_list
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_to_TOA5(
        data: pd.DataFrame, headers: pd.DataFrame, output_path: pathlib.Path
        ) -> None:
    """
    Write the data to the output file.

    Args:
        data: data to write to file.
        headers: header to write to file.
        output_path: path to write file to.

    Returns:
        None.

    """

    headers = io.reformat_headers(headers=headers, output_format='TOA5')
    data = io.reformat_data(data=data, output_format='TOA5')
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
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
