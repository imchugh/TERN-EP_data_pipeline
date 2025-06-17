#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 16 10:05:24 2025

@author: imchugh
"""

import pandas as pd

# def test(ds, target_height):


#     # Inits
#     not_ends_with = ['QCFlag', 'Sd', 'Ct']
#     int_extractor = lambda height: float(height.replace('m', ''))
#     rslt = {}

#     df = pd.DataFrame(
#         index=pd.Index([], name='height'), columns=['Ta', 'RH', 'AH']
#         )
#     for quantity in df.columns:
#         for var in ds.variables:
#             if not var.startswith(quantity):
#                 continue
#             if any([var.endswith(this) for this in not_ends_with]):
#                 continue
#             height = int_extractor(ds[var].attrs['height'])
#             inst = ds[var].attrs['instrument']
#             df.loc[height, quantity] = inst

#     return df

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

    return {value: key for key, value in rslt.items() if value is not None}





