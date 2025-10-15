#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 10 14:09:01 2025

@author: imchugh
"""

import datetime as dt
import pandas as pd
import pathlib

from configobj import ConfigObj

ALLOWED_CONFIGS = ['RangeCheck', 'ExcludeDates']
DATE_FORMAT = '%Y-%m-%d %H:%M'

#------------------------------------------------------------------------------
class L2ConfigsEditor():

    #--------------------------------------------------------------------------    
    def __init__(self, input_file: str | pathlib.Path):
        
        self.input_file = pathlib.Path(input_file)
        self.config=ConfigObj(input_file)
        breakpoint()
        if not self.config['level'] == 'L2':
            raise TypeError('Control file level must be L2!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------        
    def get_variable_list(self) -> list:
        
        return list(self.config['Variables'].keys())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------    
    def get_variable_configs(self, variable:str, config_field:str=None) -> dict:
        
        if config_field is None:
            return self.config['Variables'][variable]
        if not config_field in ALLOWED_CONFIGS:
            raise TypeError(
                f'config_field must be one of {", ".join(ALLOWED_CONFIGS)}'
                )
        return self.config['Variables'][variable][config_field]
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def set_variable_range(
            self, 
            variable: str, 
            lower: int | float | str, 
            upper: int | float | str
            ) -> None:
        
        # Check type
        [float(element) for element in [lower, upper]]
        
        # Set the range for the passed variable
        self.config['Variables'][variable]['RangeCheck'] = (
            {'lower': str(lower), 'upper': str(upper)}
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def set_date_exclusions(
            self,
            variable: str, 
            first_date: str | dt.datetime, 
            last_date: str | dt.datetime
            ):
        """
        

        Args:
            variable (str): DESCRIPTION.
            first_date (str | dt.datetime): DESCRIPTION.
            last_date (str | dt.datetime): DESCRIPTION.

        Returns:
            None.

        """

        # Handle newly passed dates
        if isinstance(first_date, dt.datetime):            
            first_date = first_date.strftime(DATE_FORMAT)
        if isinstance(last_date, dt.datetime):
            last_date = last_date.strftime(DATE_FORMAT)
        new_exclude_str = {'0': [first_date, last_date]}
        new_dates_df = _order_dates(exclude_dates=new_exclude_str)       

        # Handle existing dates in the config file
        try:
            existing_dates = self.config['Variables'][variable]['ExcludeDates']
            ex_dates_df = _order_dates(exclude_dates=existing_dates)
            final_df = _combine_dates(df1=ex_dates_df, df2=new_dates_df)
        except KeyError:
            final_df = new_dates_df

        # Reformat to dict of strings
        rslt = _format_dates(dates_df=final_df)

        # Edit configs 
        self.config['Variables'][variable]['ExcludeDates'] = rslt
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_configs(self, output_file):
        
        if pathlib.Path(output_file) == self.input_file:
            raise FileExistsError(
                'Cannot overwrite existing configuration file!'
                )
        self.config.filename = output_file
        self.config.write()
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _order_dates(exclude_dates: dict) -> pd.DataFrame:

    # Make a dataframe
    rslt = []
    for index, dates in exclude_dates.items():
        rslt.append([
            int(index), 
            dt.datetime.strptime(dates[0], DATE_FORMAT), 
            dt.datetime.strptime(dates[1], DATE_FORMAT)
            ])
    dates_df = (
        pd.DataFrame(rslt, columns=['index', 'begin', 'end'])
        .sort_values('index')
        )
    
    # Check:
        # 1) the index numbers are ordered correctly for the columns
        # 2) all minutes are either 00 or 30
    for col in ['begin', 'end']:
        if not dates_df.equals(dates_df.sort_values(col)):
            raise RuntimeError(
                'Integer index order of date exclusions does not map to '
                f'{col} dates!'
                )
        if any(dates_df.end.dt.minute % 30 != 0):
            raise RuntimeError('Minutes must be either 30 or 0!')

    # Check the index numbers are monotonically increasing
    if not dates_df['index'].is_monotonic_increasing:
        raise RuntimeError('Index values must monotonically increase!')
        
    # Check the index increment is always one (for multi-row dfs only)
    if len(dates_df) > 1:
        if not dates_df['index'].diff().nunique() == 1:
            raise RuntimeError(
                'Integer index must always increase by exactly 1!'
                )
            
    # Check that the date in the 'end' column always succeeds (or equals) 
    # that of 'start'
    if not all(dates_df['end'] >= dates_df['begin']):
        raise RuntimeError(
            'At least one of the date exclusions is out of order!'
            )
    
    # Check that there is no overlap between exclusion instances
    if any(dates_df.end.shift() > dates_df.begin):
        raise RuntimeError('Date overlap between date exclusions detected!')
    
    # Return data indexed 
    return dates_df.set_index(keys='index')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _combine_dates(df1, df2):
    
    # Combine dataframes and sort by start column
    df = pd.concat([df1, df2]).sort_values('begin')
    
    if not df.equals(df.sort_values('end')):
        breakpoint()
    
    return df.reset_index(drop=True).reset_index()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _format_dates(dates_df):
    
    # Format to strings and return a dict in the correct format
    df = dates_df.copy()
    df['index'] = df['index'].astype(str)
    for col in ['begin', 'end']:
        df[col] = df[col].dt.strftime(DATE_FORMAT)
    rslt = {}
    for i in range(len(df)):
        rslt[str(i)] = df[['begin', 'end']].iloc[i].to_list()
    return rslt
#------------------------------------------------------------------------------    
    