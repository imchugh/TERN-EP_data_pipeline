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
FLUX_FILE_VAR_IND = ['Uz_SONIC_Av', 'Tv_SONIC_Av']
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

    # Close dataset
    ds.close()

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

    # Drop extraneous T and RH variables
    drop_list += _get_extraneous_met(ds=ds)

    # Remove all extraneous variables
    return ds.drop(drop_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_extraneous_met(ds: xr.Dataset) -> list:
    """
    Choose a set of T / RH / AH variables to remove. Selectthose to keep on the
    basis of two considerations: i) same instrument; ii) closest to flux
    height. Then remove all else.

    Args:
        ds: the dataset.

    Raises:
        RuntimeError: raised if no candidates are found that meet criteria.

    Returns:
        drop_list: list of the variables that are extraneous.

    """

    # Inits
    not_ends_with = ['QCFlag', 'Sd', 'Ct']
    int_extractor = lambda height: float(height.replace('m', ''))
    target_height = None
    for var in FLUX_FILE_VAR_IND:
        try:
            target_height = int_extractor(ds[var].attrs['height'])
        except KeyError:
            continue
    if target_height is None:
        raise KeyError(
            'Neither of the flux file indicator variables '
            f'{", ".join(FLUX_FILE_VAR_IND)} were found in the file!'
            )

    # Make df
    df = pd.DataFrame(
        index=pd.Index([], name='variable'),
        columns=['height_diff', 'quantity', 'instrument']
        )

    # Fill df
    for quantity in ['Ta', 'RH', 'AH']:
        for var in ds.variables:
            if not var.startswith(quantity):
                continue
            if any(var.endswith(this) for this in not_ends_with):
                continue
            if 'IRGA' in var or 'SONIC' in var:
                continue
            df.loc[var] = [
                abs(int_extractor(ds[var].attrs['height']) - target_height),
                quantity,
                ds[var].attrs['instrument']
                ]

    # Isolate temperature data
    ta_df = df.loc[df.quantity=='Ta'].sort_values('height_diff')

    # Parse data to find RH and/or AH instrument match at closest height to flux
    rslt = {'Ta': None, 'RH': None, 'AH': None}
    fallback_rslt = {'Ta': None, 'RH': None, 'AH': None}
    use_main = False
    for variable in ta_df.index:
        height_diff, instrument = (
            ta_df.loc[variable, ['height_diff', 'instrument']].tolist()
            )
        rslt['Ta'] = variable
        fallback_rslt['Ta'] = variable
        for quantity in ['RH', 'AH']:

            # Check for same instrument at same height for a given quantity
            sub_df = df.loc[
                (df.height_diff == height_diff) &
                (df.quantity == quantity) &
                (df.instrument == instrument)
                ]
            if not len(sub_df) == 0:
                rslt[quantity] = sub_df.index[0]
                continue

            # As a fallback, check for ANOTHER instrument at same height
            sub_df = df.loc[
                (df.height_diff == height_diff) &
                (df.quantity == quantity)
                ]
            if not len(sub_df) == 0:
                fallback_rslt[quantity] = sub_df.index[0]
                continue

        # Break as soon as there is a hit
        if any(rslt[qtty] is not None for qtty in ['RH', 'AH']):
            use_main = True
            break

    # Use the fallback result if the main is no good - or throw an error if
    # there is no fallback
    if not use_main:
        if any(fallback_rslt[qtty] is not None for qtty in ['RH', 'AH']):
            rslt = fallback_rslt
        else:
            raise RuntimeError(
                'Could not find instrument at same level as temperature '
                'measurement!'
                )

    # Get the drop list and return
    keep_list = [value for value in rslt.values() if not value is None]
    drop_list = [var for var in df.index if not var in keep_list]

    return drop_list
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
                and not 'SONIC' in var
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
