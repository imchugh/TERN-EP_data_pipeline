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
import numpy as np
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


    # Get flux measurement height
    int_extractor = lambda height: float(height.replace('m', ''))
    target_height = int_extractor(ds[FLUX_FILE_VAR_IND].attrs['height'])

    # Drop extraneous temp / RH / AH variables
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
def test(ds, target_height):

    # Inits
    not_ends_with = ['QCFlag', 'Sd', 'Ct']
    int_extractor = lambda height: float(height.replace('m', ''))

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
            if 'IRGA' in var:
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

# #------------------------------------------------------------------------------
# def _sort_met_vars(ds: xr.Dataset, target_height) -> xr.Dataset:

#     # Inits
#     not_ends_with = ['QCFlag', 'Sd', 'Ct']
#     int_extractor = lambda height: float(height.replace('m', ''))
#     rslt = {}

#     def get_df(quantity):

#         rslt = []
#         for var in ds.variables:
#             if not var.startswith(quantity):
#                 continue
#             if any([var.endswith(this) for this in not_ends_with]):
#                 continue
#             rslt.append(
#                 {
#                     'variable': var,
#                     'height_diff': abs(
#                         target_height -
#                         int_extractor(ds[var].attrs['height'])
#                         ),
#                     'instrument': ds[var].attrs['instrument']
#                     }
#                 )

#         if len(rslt) != 0:
#             return (
#                 pd.DataFrame(rslt)
#                 .set_index(keys='variable')
#                 .sort_values('height_diff')
#                 )

#     ta_df = get_df(quantity='Ta')
#     rh_df = get_df(quantity='RH')
#     ah_df = get_df(quantity='AH')


#     for var in ah_df.index:
#         if 'IRGA' in var:
#             ah_df = ah_df.drop(var)

#     keep_ta, keep_rh, keep_ah = None, None, None
#     for diff in ta_df.height_diff:

#         ta_subdf = ta_df.loc[ta_df.height_diff==diff]
#         keep_ta = ta_subdf.index.item()

#         try:
#             rh_subdf = rh_df.loc[rh_df.height_diff==diff]
#             if ta_subdf.instrument.item() == rh_subdf.instrument.item():
#                 keep_rh = rh_subdf.index.item()
#         except (KeyError, ValueError):
#             pass

#         try:
#             ah_subdf = ah_df.loc[ah_df.height_diff==diff]
#             if ta_subdf.instrument.item() == ah_subdf.instrument.item():
#                 keep_ah = ah_subdf.index.item()
#         except (KeyError, ValueError):
#             pass

#         if keep_rh is not None or keep_ah is not None:
#             break

#     if keep_rh is None and keep_ah is None:
#         raise RuntimeError(
#             'An independent temperature and humidity probe is required!'
#             )

#     return {'Ta': keep_ta, 'RH': keep_rh, 'AH': keep_ah}

#     #     heights = [
#     #         abs(target_height - int_extractor(ds[var].attrs['height']))
#     #         for var in var_list
#     #         ]
#     #     return (
#     #         pd.DataFrame(
#     #             data={
#     #                 'height_diff': [
#     #                     [
#     #                         abs(target_height - int_extractor(ds[var].attrs['height']))
#     #                         for var in var_list
#     #                         ],
#     #                 'instrument': [ds[var].attrs['instrument'] for var in ta_vars]
#     #                 },
#     #             index=ta_vars
#     #             )
#     #         .sort_values('height_diff')
#     #         )

#     # ta_vars = get_vars('Ta')
#     # df = (
#     #     pd.DataFrame(
#     #         data={
#     #             'height_diff': get_heights(var_list=ta_vars),
#     #             'instrument': [ds[var].attrs['instrument'] for var in ta_vars]
#     #             },
#     #         index=ta_vars
#     #         )
#     #     .sort_values('height_diff')
#     #     )

#     # breakpoint()

#     # height_list = get_heights(var_list=ta_vars)
#     # idx = np.argsort(height_list)
#     # ta_vars = np.array(ta_vars)[idx].tolist()


#     # for quantity in ['RH', 'AH']:
#     #     var_list = get_vars(quantity=quantity)
#     #     # for var in var_list:





#     # Get all the temperature variables
#     for var in ds.variables:
#         if var.startswith('Ta'):
#             if not any([var.endswith(this) for this in not_ends_with]):
#                 pass


#     #             ta_dict[var] = (
#     #                 target_height - int_extractor(ds[var].attrs['height'])
#     #                 )
#     # ta_dict = {
#     #     key: value for key, value in sorted(
#     #         ta_dict.items(), key= lambda item: item[1]
#     #         )
#     #     }

#     # for var in ta_dict.keys():


#     # return ta_dict



#     # Find the met variables in each dataset - exclude IRGA measurements of AH
#     # (deal with separately)
#     for var in ['Ta', 'RH', 'AH']:
#          temp_list = []
#          for ds_var in ds.variables:
#              if var == 'AH':
#                  if 'IRGA' in ds_var:
#                      continue
#              if ds_var.startswith(var):
#                  if not any(
#                      [ds_var.endswith(not_this) for not_this in not_ends_with]
#                      ):
#                      temp_list.append(ds_var)

#          # Move on if not variables
#          if len(temp_list) == 0:
#              continue

#          # Otherwise calculate heights
#          heights_list = [
#              target_height - int_extractor(ds[var].attrs['height'])
#              for var in temp_list
#              ]

#          # Sort ascending by height
#          idx = np.argsort(heights_list)
#          rslt[var] = dict(zip(
#              np.array(heights_list)[idx].tolist(),
#              np.array(temp_list)[idx].tolist()
#              ))

#     # Check first height has an equivalent for each of RH and AH
#     Ta_dict = rslt.pop('Ta')

#     for height, var in Ta_dict.items():
#         inst = ds[var].attrs['instrument']
#         for quantity in rslt.keys():
#             try:
#                 var_name = rslt[quantity][height]
#                 this_inst = ds[var_name]['instrument']

#             except KeyError:
#                 pass





#          # temp_rslt = dict(zip(temp_list, heights_list))
#          # rslt[var] = {
#          #     key: value for key, value in sorted(
#          #         temp_rslt.items(), key= lambda item: item[1]
#          #         )
#          #     }

#     # # Sort each
#     # for var in rslt:
#     #     rslt[var] = {
#     #         key: value for key, value in sorted(
#     #             rslt['Ta'].items(), key= lambda item: item[1]
#     #             )
#     #         }



#     # keep_var = min(var_dict, key=var_dict.get)
#     # for key in list(rslt['AH'].keys()):
#     #     if 'IRGA' in key:
#     #         rslt['AH'].pop(key)
#     # if len(rslt['AH']) == 0:
#     #     rslt.pop('AH')
#     return rslt
# #------------------------------------------------------------------------------

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
