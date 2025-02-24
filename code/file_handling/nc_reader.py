#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 09:08:18 2025

@author: imchugh
"""

import pandas as pd
import pathlib
import xarray as xr


###############################################################################
### BEGIN NETCDF READER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class NCReader():
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

        ds = xr.open_dataset(nc_file)
        self.ds = ds
        ds.close()
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

    # #--------------------------------------------------------------------------
    # def get_headers(self) -> pd.core.frame.DataFrame:
    #     """
    #     Create the header dataframe required for the TOA5 conversion.

    #     Returns:
    #         dataframe.

    #     """

    #     return pd.DataFrame(
    #         data = [
    #             {
    #                 'units': self.ds[var].attrs['units'],
    #                 'sampling':
    #                     STATISTIC_ALIASES[self.ds[var].attrs['statistic_type']]
    #                 }
    #             for var in self.labels_to_keep
    #             ],
    #         index=self.labels_to_keep
    #         )
    # #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END NETCDF READER CLASS ###
###############################################################################