#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 14:11:18 2020

@author: imchugh
"""

# Base python imports
import datetime as dt
import pandas as pd

# Custom imports
from file_handling import file_handler as fh
from paths import paths_manager as pm

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

INTAKE_HEIGHTS = [0.5, 1, 2, 3.5, 7, 12, 20, 29]
CO2_LIMITS = [300, 1000]
PROFILE_FILE_NAME = 'CUP_AUTO_co2profile.dat'
PROFILE_VARS_TO_IMPORT = ['CO2_Avg', 'ValveNo']
TEMP_FILE_NAME = 'CUP_AUTO_climate.dat'
TEMP_VARS_TO_IMPORT = ['Ta_HMP_01_Avg', 'Ta_HMP_155_Avg']
T_AIR_HEIGHTS = {
    'upper': {'name': 'Ta_HMP_155_Avg', 'height': 30},
    'lower': {'name': 'Ta_HMP_01_Avg', 'height': 7}
    }
P_FILE_NAME = 'CumberlandPlain_EP_MASTER.txt'
P_VARS_TO_IMPORT = ['air_pressure', 'air_temperature']

#------------------------------------------------------------------------------

# profile_data_path = pm.get_local_stream_path(
#     resource='raw_data', 
#     stream='profile',
#     site='CumberlandPlain',
#     file_name=PROFILE_FILE_NAME
#     )

# temp_data_path = pm.get_local_stream_path(
#     resource='raw_data', 
#     stream='flux_slow',
#     site='CumberlandPlain',
#     file_name=TEMP_FILE_NAME
#     )

# press_data_path = pm.get_local_stream_path(
#     resource='raw_data', 
#     stream='profile',
#     site='CumberlandPlain',
#     file_name=P_FILE_NAME
#     )

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def apply_limits(df, limits):

    return df.where((df >= limits[0]) & (df <= limits[1]))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_pressure(df):

    return df * 10**-3
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def interpolate_T(df):

    upper_ht = T_AIR_HEIGHTS['upper']['height']
    upper_name = T_AIR_HEIGHTS['upper']['name']
    lower_ht = T_AIR_HEIGHTS['lower']['height']
    lower_name = T_AIR_HEIGHTS['lower']['name']
    dtdz = (df[upper_name] - df[lower_name]) / (upper_ht - lower_ht)
    return pd.concat(
        [df[lower_name] + (ht - lower_ht) * dtdz for ht in INTAKE_HEIGHTS],
        axis=1
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def stack_to_series(df, name):

    stacked_series = df.stack(future_stack=True)
    stacked_series.name = name
    stacked_series.index.names = ['Time', 'Height']
    return stacked_series
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def timestack_co2_data(df):

    # Increment the time to land on the half hour
    df.index += dt.timedelta(seconds=1)

    # Drop records where valve number and logger time were not in proper sync
    df = (
        df.drop(df[~(df.index.minute % 30 == df.ValveNo)].index)
        .drop_duplicates()
        )

    # Map valves to heights, then align all valves in time
    df['Time'] = [i - dt.timedelta(minutes = i.minute % 30) for i in df.index]
    valve_map_dict = dict(zip(sorted(df.ValveNo.unique()), INTAKE_HEIGHTS))
    df['Height'] = df.ValveNo.apply(lambda x: valve_map_dict[x])

    # Create a 2D index (D1=time, D2=height) and a new 30minute time index
    df.index = pd.MultiIndex.from_frame(df[['Time', 'Height']])
    new_idx = pd.date_range(df.index.get_level_values(0)[0],
                            df.index.get_level_values(0)[-1], freq='30min')

    # Break out the CO2 time series to separate heights and reindex the time
    return df.CO2_Avg.unstack().reindex(new_idx)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data(stream, file_name, use_cols=None):

    path = pm.get_local_stream_path(
        resource='raw_data', 
        stream=stream,
        site='CumberlandPlain',
        file_name=file_name
        )
    return (
        fh.DataHandler(file=path)
        .get_conditioned_data(usecols=use_cols)
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN FUNCTION ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def return_data():

    # Get CO2 data
    profile_df = get_data(
        stream='profile', 
        file_name=PROFILE_FILE_NAME, 
        use_cols=PROFILE_VARS_TO_IMPORT
        )

    # Construct CO2 series
    co2_series = (
        timestack_co2_data(df=profile_df)
        .pipe(stack_to_series, 'CO2')
        .pipe(apply_limits, CO2_LIMITS)
        )

    # Get temperature data
    temp_df = get_data(
        stream='flux_slow', 
        file_name=TEMP_FILE_NAME, 
        use_cols=TEMP_VARS_TO_IMPORT
        )
    
    # Construct temperature series
    ta_series = (
        interpolate_T(df=temp_df)
        .rename(dict(enumerate(INTAKE_HEIGHTS)), axis=1)
        .pipe(stack_to_series, 'Tair')
        )

    # Get pressure data
    p_df = get_data(
        stream='flux_slow', 
        file_name=P_FILE_NAME, 
        use_cols=P_VARS_TO_IMPORT
        )

    # Construct pressure series
    p_series = (
        pd.concat([p_df['air_pressure'].copy() for i in range(8)], axis=1,
                  ignore_index=True)
        .rename(dict(enumerate(INTAKE_HEIGHTS)), axis=1)
        .pipe(stack_to_series, 'P')
        .pipe(convert_pressure)
        )
    
    # Calculate the earliest of the 3 timestamps
    earliest_ts = min(
        co2_series.index[-1][0],
        ta_series.index[-1][0],
        p_series.index[-1][0]
        )

    # Concatenate xarray dataset, assign variable attrs and return
    ds = (
        pd.concat([co2_series, ta_series, p_series], axis=1)
        .loc[:earliest_ts]
        .to_xarray()
        )
    ds.CO2.attrs = {'units': 'umol/mol'}
    ds.Tair.attrs = {'units': 'degC'}
    ds.P.attrs = {'units': 'kPa'}
    return ds
#------------------------------------------------------------------------------
