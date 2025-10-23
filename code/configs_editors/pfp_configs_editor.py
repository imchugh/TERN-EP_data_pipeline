#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 10 14:09:01 2025

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

from configobj import ConfigObj
import datetime as dt
import pathlib

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

ALLOWED_CONFIGS = ['RangeCheck', 'ExcludeDates']
DATE_FORMAT = '%Y-%m-%d %H:%M'

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN CLASSES ###
###############################################################################

#------------------------------------------------------------------------------
class L2ConfigsEditor():
    """Editor for PFP L2 configuration files"""

    #--------------------------------------------------------------------------    
    def __init__(self, input_file: str | pathlib.Path) -> None:
        """
        Import the configuration information.

        Args:
            input_file: name of file containing configs.

        Raises:
            TypeError: raised if control file is not L2.

        Returns:
            None.

        """
        
        self.input_file = pathlib.Path(input_file)
        self.config=ConfigObj(input_file)
        if not self.config['level'] == 'L2':
            raise TypeError('Control file level must be L2!')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------        
    def get_variable_list(self) -> list:
        """
        List the variables in the configuration file.

        Returns:
            the list.

        """
        
        return list(self.config['Variables'].keys())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------    
    def get_variable_configs(self, variable:str, config_field:str=None) -> dict:
        """
        Return the variable configs.

        Args:
            variable: name of variable for which to return configs.
            config_field (optional): supply field name if a specific variable 
            is required. Defaults to None.

        Raises:
            TypeError: raised if a non-standard config name is passed..

        Returns:
            dict containing the configs.

        """
        
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
        """
        Set the upper and lower range limits for a variable.

        Args:
            variable: name of variable for which to return configs.
            lower: minimum value.
            upper: maximum value.

        Returns:
            None.

        """
        
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
        Set the date ranges to exclude for a variable.

        Args:
            variable: name of variable for which to return configs.
            first_date: first date to exclude (inclusive).
            last_date: last date to exclude (inclusive).

        Returns:
            None.

        """

        # Handle newly passed dates
        if isinstance(first_date, dt.datetime):            
            first_date = first_date.strftime(DATE_FORMAT)
        if isinstance(last_date, dt.datetime):
            last_date = last_date.strftime(DATE_FORMAT)

        # Convert to a list of datetime.date pairs
        range_list = self.get_variable_configs(
            variable=variable,
            config_field='ExcludeDates'
            ).values()
        range_list.append([first_date, last_date])
        fmt_l = [
            (
                dt.datetime.strptime(date_range[0], DATE_FORMAT), 
                dt.datetime.strptime(date_range[1], DATE_FORMAT)
                ) 
            for date_range in range_list
            ]
        
        # Merge the dates
        rslt = _combine_dates(ranges=fmt_l)
        
        # Convert to a list of str date pairs
        out_dict = {
            str(i):
                [
                    date_range[0].strftime(DATE_FORMAT),
                    date_range[1].strftime(DATE_FORMAT)
                    ] 
                for i, date_range in enumerate(rslt)
            }
        
        # Write back to the config
        self.config['Variables'][variable]['ExcludeDates'] = out_dict
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_configs(self, output_file: str | pathlib.Path) -> None:
        """
        Write the configs to an output file.

        Args:
            output_file: file to write to.

        Raises:
            FileExistsError: raised if user attempts to overwrite existing file.

        Returns:
            None.

        """
        
        if pathlib.Path(output_file) == self.input_file:
            raise FileExistsError(
                'Cannot overwrite existing configuration file!'
                )
        self.config.filename = output_file
        self.config.write()
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _combine_dates(ranges: list) -> list:
    """
    Sort and handle overlapping dates.

    Args:
        ranges: list of str date pairs.

    Returns:
        revised list of datetime date pairs.

    """
    
    # Sort by start date
    ranges.sort(key=lambda x: x[0])
    
    # Merge overlaps
    merged = []
    for start, end in ranges:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    
    # Return list of two-tuples
    return [(s, e) for s, e in merged]
#------------------------------------------------------------------------------

###############################################################################
### END CLASSES ###
###############################################################################
   