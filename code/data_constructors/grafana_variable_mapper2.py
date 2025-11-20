#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 24 11:30:48 2025

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import numpy as np
import pandas as pd
import pathlib

# -----------------------------------------------------------------------------

from file_handling import file_io as io
from file_handling import nc_io as ncio
from managers import paths
from managers import metadata

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

HEIGHT_VAR = 'Ws_SONIC_Av'
NOT_ENDS_WITH = ['QC_Flag', 'QC', 'Sd', 'Ct']
ADD_ATTRS = ['quantity', 'horizontal_location', 'replicate']
nc_path = paths.get_local_stream_path(resource='homogenised_data', stream='nc')
parser = metadata.PFPNameParser()

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def map_all_sites(output_path: str | pathlib.Path=None) -> dict:

    # Get the directory containing the config files
    configs_path = paths.get_local_stream_path(
        resource='configs', stream='site_config_files'
        )
    
    # Parse the config files
    rslt = {}
    for path in sorted(configs_path.glob('*.yml')):
        site = path.stem
        print (f'Running site {site}...')
        rslt.update({site: map_variables(site=site)})

    # Spit out to json if output path supplied
    if output_path is not None:
        output_path = pathlib.Path(output_path)
        if not output_path.parent.exists():
            raise FileNotFoundError(f'Directory {path.parent} does not exist!')
        io.write_json(file=output_path, data=rslt)
                
    return rslt
    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def map_variables(site: str) -> dict:
    """
    Map the old site-specific variable names to generic names

    Args:
        site: name of site.

    Returns:
        dictionary mapping old -> new names.

    """

    # Get the nc file for the site
    reader = ncio.NCReader(nc_path / site / f'{site}_2025_L1.nc')
    temp_dict = reader.variable_attrs.T.to_dict()
    drop_list = []
    for variable in temp_dict.keys():
        try:
            temp_dict[variable].update(
                parser.parse_variable_name(variable_name=variable)
                )
        except TypeError:
            drop_list.append(variable)
    temp_dict = {
        key: value for key, value in temp_dict.items() if not key in drop_list
        }
    df = pd.DataFrame.from_dict(temp_dict).T.fillna('')
       
    # Copy the underlying dataframe from the manager and amend heights to float
    # df = md_mngr.site_variables.copy()
    df['height'] = df['height'].apply(height_extractor)

    # Result dict to contain variable translations
    rslt = {}

    # Add the met variable translations
    rslt.update(_get_met_vars(df=df))

    # Add the non-met aboveground variable translations
    rslt.update(_get_non_met_ag_vars(df=df))

    # Add the belowground variable translations
    rslt.update(_get_bg_vars(df=df))

    # Handle signal (IRGASONs use two channels - CO2 and H2O)
    rslt.update(_get_sig_vars(df=df))

    # Handle precip - some sites have multiple gauges
    rslt.update(_get_precip_vars(df=df))
    
    # Remove all redundant variables
    rslt = {key: value for key, value in rslt.items() if not key == value}

    return rslt
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def height_extractor(height_str: str) -> float:
    """
    Get a float representation of height / depth from config file text string.

    Args:
        height_str: the height string.

    Raises:
        RuntimeError: raised if parsing fails.

    Returns:
        float representation of config file string.

    """

    if height_str == '':
        return np.nan
    rslt_list = height_str.replace('m', '').split('to')
    if len(rslt_list) == 1:
        try:
            return float(rslt_list[0])
        except ValueError:
            breakpoint()
    if len(rslt_list) == 2:
        return sum([float(rslt_list[0]), float(rslt_list[1])]) / 2
    raise RuntimeError('Too many height elements!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_met_vars(df: pd.DataFrame) -> dict:
    """
    Find the met variables at closest proximity to flux measurement height.

    Args:
        df: dataframe containing configuration information.

    Returns:
        dictionary mapping of meteorological variables.

    """

    # Create a temperature dataframe and order by target height delta
    target_height = df.loc[HEIGHT_VAR, 'height']
    ta_df = df.loc[df.quantity == 'Ta'].copy()
    ta_df['height_diff'] = abs(ta_df.height - target_height)
    ta_df = ta_df.sort_values('height_diff')

    # Iterate over temperature variables to find one with a matching RH value
    for variable in ta_df.index:

        # Initialise the result dictionary        
        rslt = {'Ta': None, 'RH': None, 'AH': None}        
        height_diff, instrument = (
            ta_df.loc[variable, ['height_diff', 'instrument']].tolist()
            )
        rslt['Ta'] = variable

        # Iterate over AH and RH
        for quantity in ['RH', 'AH']:
    
            # Create a dataframe for quantity and order by target height delta
            sub_df = df.loc[df.quantity == quantity].copy()
            if len(sub_df) == 0:
                continue
            sub_df['height_diff'] = abs(sub_df.height - target_height)
            sub_df = sub_df.sort_values('height_diff')
    
            # Check for same instrument at same height
            try:
                match_var = sub_df.loc[
                    (sub_df.height_diff == height_diff) &
                    (sub_df.quantity == quantity) &
                    (sub_df.instrument == instrument)
                    ].index.item()
                rslt[quantity] = match_var
                continue
            except ValueError:
                continue
    
            # If not found, drop the instrument match requirement
            match_var = sub_df.loc[
                (sub_df.height_diff == height_diff) &
                (sub_df.quantity == quantity)
                ].index.item()
            if not len(match_var) == 0:
                rslt[quantity] = match_var
                continue
    
            # If not found, drop the height match requirement and accept the
            # smallest delta
            rslt[quantity] = sub_df.height_diff.idxmin()
            continue
    
            # Break as soon as there is a hit
            if not None in rslt.values():
                break

    # Remove fields with empty values, reverse and return
    return {value: key for key, value in rslt.items() if value is not None}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_non_met_ag_vars(df):

    AG_control_list = ['Fco2', 'Fe', 'Fh', 'ustar', 'Vbat', 'Tpanel']

    # Map other aboveground variables
    rslt = {}

    for qtty in AG_control_list:

        if qtty in df.index:
            rslt[qtty] = qtty; continue

        sub_df = df.loc[df.quantity == qtty]
        sub_df = sub_df[~sub_df.index.str.endswith(tuple(NOT_ENDS_WITH))]
        if len(sub_df) == 1:
            rslt[sub_df.index[0]] = qtty; continue
        if len(sub_df) == 0:
            error_msg = f'Could not find any variables for quantity {qtty}!'
        if len(sub_df) > 1:
            rslt[sub_df.replicate.idxmin()] = qtty; continue
            error_msg = (
                f'Too many variables ({", ".join(sub_df.index.tolist())}) '
                f'for generic variable {qtty}'
                )
        raise IndexError(error_msg)
    return rslt
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_bg_vars(df):

    rslt = {}
    fg_vars = df[df.quantity=='Fg'].index.tolist()
    rslt.update({var: f'Fg_{i + 1}' for i, var in enumerate(fg_vars)})
    for var in ['Ts', 'Sws']:
        sub_df = df.loc[df.quantity == var]
        for i, fg_var in enumerate(fg_vars):
            hor_loc = df.loc[fg_var, 'horizontal_location']
            vert_loc = df.loc[fg_var, 'height']
            try:
                nearest_var = abs(
                    sub_df.loc[sub_df.horizontal_location==hor_loc, 'height'] -
                    vert_loc
                    ).idxmin()
            except ValueError:
                continue
            rslt.update({nearest_var: f'{var}_{i + 1}'})
    return rslt
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_sig_vars(df):
    
    if 'Sig_IRGA' in df.index:
        return {}
    if 'SigCO2_IRGA' in df.index:
        return {'SigCO2_IRGA': 'Sig_IRGA'}
    if 'SigH2O_IRGA' in df.index:
        return {'SigH2O_IRGA': 'Sig_IRGA'}
    return {}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_precip_vars(df):
    
    if 'Precip' in df.index:
        return {}
    precip_list = sorted([var for var in df.index if 'Precip' in var])
    try:
        return {precip_list[0]: 'Precip'}
    except IndexError:
        return {}
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
