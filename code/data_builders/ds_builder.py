# -*- coding: utf-8 -*-
"""
Created on Thu May 16 16:25:29 2024

@author: imchugh

This module is used to merge data (from multiple raw data file sources) and
metadata (from configuration files and TERN DSA database) into xarray datasets
and netcdf files (only L1 so far).

Module classes:
    - L1DataBuilder - builds an xarray dataset with data and metadata required
    to generate L1 files.
    - NCConverter - used to extract data from an existing .nc file and push
    back out to Campbell Scientific TOA5 format data file.

Module functions:
    - make_nc_file - generates the xarray dataset and writes out to nc file.
    - make_nc_year_file - as above, but confined to a single data year.
    - append_to_current_nc_file - checks for a current-year .nc file - appends
    if it exists, generates it if it doesn't.
    - append_to_nc - generic append function where existing file is passed as
    arg.

"""

import datetime as dt
import numpy as np
import pandas as pd
import pathlib
import xarray as xr

import data_builders.data_merger as dm
import utils.configs_manager as cm
import file_handling.file_io as io
import utils.metadata_handlers as mh

#------------------------------------------------------------------------------
SITE_DETAIL_ALIASES = {'elevation': 'altitude'}
SITE_DETAIL_SUBSET = [
    'fluxnet_id', 'latitude', 'longitude', 'elevation', 'time_step', 'time_zone'
    ]
VAR_METADATA_SUBSET = [
    'height', 'instrument', 'long_name', 'standard_name', 'statistic_type',
    'units'
    ]
STATISTIC_ALIASES = {'average': 'Avg', 'variance': 'Vr', 'sum': 'Tot'}
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
#------------------------------------------------------------------------------

###############################################################################
### BEGIN L1 DATA BUILDER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class L1DataBuilder():

    #--------------------------------------------------------------------------
    def __init__(
            self, site: str, md_mngr: mh.MetaDataManager=None,
            concat_files: bool=False
            ) -> None:
        """
        Get the data and metadata.

        Args:
            site: name of site.
            md_mngr (optional): existing MetaDataManager. Defaults to None.
            concat_files (optional): If true, concatenate the raw data files
            and historical backups. If not, not. Defaults to False.

        Returns:
            None.

        """

        self.site = site
        if md_mngr is None:
            md_mngr = mh.MetaDataManager(site=site)
        self.md_mngr = md_mngr

        # Merge the raw data (no corrections applied)
        merge_dict = {
            file: self.md_mngr.translate_variables_by_table(table=table)
            for table, file in self.md_mngr.map_tables_to_files(abs_path=True).items()
            }
        self.data = (
            dm.merge_all(
                files=merge_dict, concat_files=concat_files
                )
            ['data']
            )
        self.data_years = self.data.index.year.unique().tolist()
        self.global_attrs = self._get_site_global_attrs()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_site_global_attrs(self) -> dict:
        """
        Combine default and site-specific info to create preliminary global
        attrs.

        Returns:
            TYPE: DESCRIPTION.

        """

        global_attrs = cm.get_global_configs(which='nc_generic_attrs')
        new_dict = {
            'metadata_link':
                global_attrs['metadata_link'].replace('<site>', self.site),
            'site_name': self.site
            }
        global_attrs.update(new_dict)

        site_specific_attrs = (
            self.md_mngr.site_details
            [SITE_DETAIL_SUBSET]
            .rename(SITE_DETAIL_ALIASES)
            .to_dict()
            )
        return global_attrs | site_specific_attrs
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def build_xarray_dataset_complete(self) -> xr.Dataset:
        """
        Build an xarray dataset constructed from all of the raw data.

        Returns:
            The dataset.

        """

        return self._build_xarray_dataset(df=self.data)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def build_xarray_dataset_by_slice(
            self, start_date: dt.datetime | str=None,
            end_date: dt.datetime | str=None
            ) -> xr.Dataset:
        """
        Build an xarray dataset constructed from a discrete time slice.

        Args:
            start_date (optional): the start date for the data. If None,
            starts at the beginning of the merged dataset. Defaults to None.
            end_date (optional): the end date for the data. If None,
            ends at the end of the merged dataset. Defaults to None.

        Returns:
            The dataset.

        """

        if start_date is None and end_date is None:
            return self.build_xarray_dataset_complete()
        if start_date is None:
            return self._build_xarray_dataset(
                df=self.data.loc[: end_date]
                )
        if end_date is None:
            return self._build_xarray_dataset(
                df=self.data.loc[start_date:]
                )
        return self._build_xarray_dataset(
            df=self.data.loc[start_date: end_date]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def build_xarray_dataset_by_year(self, year: int) -> xr.Dataset:

        """
        Build an xarray dataset constructed from a given year.

        Args:
            year: the data year to return.

        Returns:
            The dataset.

        """

        if not year in self.data_years:
            years = ', '.join([str(year) for year in self.data_years])
            raise IndexError(
                f'Data year is not available (available years: {years})'
                )
        bounds = self._get_year_bounds(
            year=year,
            time_step=self.global_attrs['time_step']
            )
        return self._build_xarray_dataset(
            df=self.data.loc[bounds[0]: bounds[1]]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_year_bounds(self, year: int, time_step: int) -> dict:
        """


        Args:
            year (int): DESCRIPTION.
            time_step (int): DESCRIPTION.

        Returns:
            dict: DESCRIPTION.

        """

        return (
                (
                (dt.datetime(year, 1, 1) + dt.timedelta(minutes=time_step))
                .strftime(TIME_FORMAT)
                ),
            dt.datetime(year + 1, 1, 1).strftime(TIME_FORMAT)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_xarray_dataset(self, df: pd.core.frame.DataFrame) -> xr.Dataset:
        """
        Convert the dataframe to an xarray dataset and apply global attributes.

        Args:
            df: the dataframe to convert to xr dataset.

        Returns:
            ds: xarray dataset.

        """

        # Create xarray dataset
        ds = (
            df.assign(
                latitude=self.global_attrs['latitude'],
                longitude=self.global_attrs['longitude']
                )
            .reset_index()
            .rename({'DATETIME': 'time'}, axis=1)
            .set_index(keys=['time', 'latitude', 'longitude'])
            .to_xarray()
            )

        # Assign global and variable attrs and flags
        self._assign_global_attrs(ds=ds)
        self._assign_dim_attrs(ds=ds)
        self._set_dim_encoding(ds=ds)
        self._assign_variable_attrs(ds=ds)
        self._assign_crs_var(ds=ds)
        self._assign_variable_flags(ds=ds)

        return ds
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _assign_global_attrs(self, ds) -> dict:
        """
        Augment global attributes.

        Args:
            df: pandas dataframe containing merged data.
            md_mngr: VariableManager class used to access variable attributes.

        Returns:
            Dict containing global attributes.

        """

        # Get time info to add to global attrs (note that year is lagged
        # by one time interval because the timestamps are named for the END of
        # the measurement period)
        func = lambda x: pd.to_datetime(x).strftime(TIME_FORMAT)
        year_list = (
            (
                pd.to_datetime(ds.time.values) -
                dt.timedelta(minutes=self.md_mngr.site_details.time_step)
                )
            .year
            .unique()
            )
        # pd.to_datetime(ds.time.values).year.unique()
        year_str = ''
        if len(year_list) == 1:
            year_str = f' for the calendar year {year_list[0]}'
        title_str = (
            f'Flux tower data set from the {self.site} site'
            f'{year_str}, {self.md_mngr.instruments["IRGA"]}'
            )

        # Get current time info for run
        date = dt.datetime.now()
        this_year = date.strftime('%Y')
        this_month = date.strftime('%b')

        # Assign attrs
        ds.attrs = (
            self.global_attrs |
            {
                'title': title_str,
                'date_created': date.strftime(TIME_FORMAT),
                'nc_nrecs': len(ds.time),
                'history': f'{this_month} {this_year} processing',
                'time_coverage_start': func(ds.time.values[0]),
                'time_coverage_end': func(ds.time.values[-1]),
                'irga_type': self.md_mngr.instruments['IRGA'],
                'sonic_type': self.md_mngr.instruments['SONIC']
                }
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _assign_dim_attrs(self, ds: xr.Dataset):
        """
        Apply dimension attributes (time, lattitude and longitude).

        Args:
            ds: xarray dataset.

        Returns:
            None.

        """

        dim_attrs = cm.get_global_configs(which='nc_dim_attrs')
        for dim in ds.dims:
            ds[dim].attrs = dim_attrs[dim]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _set_dim_encoding(self, ds: xr.Dataset):
        """
        Apply (so far only) time dimension encoding.

        Args:
            ds: xarray dataset.

        Returns:
            None.

        """

        ds.time.encoding['units']='days since 1800-01-01 00:00:00.0'
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _assign_variable_attrs(self, ds: xr.Dataset):
        """
        Assign the variable attributes to the existing dataset.

        Args:
            ds: xarray dataset.
            md_mngr: VariableManager class used to access variable attributes.

        Returns:
            None.

        """

        var_list = [var for var in ds.variables if not var in ds.dims]
        for var in var_list:
            ds[var].attrs = (
                self.md_mngr.get_variable_attributes(variable=var)
                [VAR_METADATA_SUBSET]
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _assign_crs_var(self, ds: xr.Dataset):
        """
        Assign coordinate reference system variable.

        Args:
            ds: xarray dataset.

        Returns:
            None.

        """

        dim_attrs = cm.get_global_configs(which='nc_dim_attrs')
        ds['crs'] = (
            ['time', 'latitude', 'longitude'],
            np.tile(np.nan, (len(ds.time), 1, 1)),
            dim_attrs['coordinate_reference_system']
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _assign_variable_flags(self, ds: xr.Dataset):
        """
        Assign the variable QC flags to the existing dataset.

        Args:
            ds: xarray dataset.

        Returns:
            None.

        """

        var_list = [var for var in ds.variables if not var in ds.dims]
        for var in var_list:
            ds[f'{var}_QCFlag'] = (
                ['time', 'latitude', 'longitude'],
                pd.isnull(ds[var]).astype(int),
                {'long_name': f'{var}QC flag', 'units': '1'}
                )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END L1 DATA BUILDER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class NCConverter():
    """
    Class to allow conversion of data from NetCDF format back to a TOA5-like file.
    This is intended to replace the current functionality for production of the
    RTMC datasets.
    """

    #--------------------------------------------------------------------------
    def __init__(self, nc_file: pathlib.Path | str) -> None:
        """
        Open the NetCDF file as xarray dataset, and set labels to keep / drop.

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
    def make_dataframe(self) -> pd.core.frame.DataFrame:
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
    def make_headers(self) -> pd.core.frame.DataFrame:
        """
        Create the header dataframe required for the TOA5 conversion.

        Returns:
            dataframe.

        """

        return pd.DataFrame(
            data = [
                {
                    'units': self.ds[var].attrs['units'],
                    'statistic_type':
                        STATISTIC_ALIASES[self.ds[var].attrs['statistic_type']]
                    }
                for var in self.labels_to_keep
                ],
            index=self.labels_to_keep
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_TOA5(self, file_path):
        """


        Args:
            file_path (TYPE): DESCRIPTION.

        Returns:
            None.

        """

        headers = io.reformat_headers(
            headers=(
                self.make_headers()
                .rename({'statistic_type': 'sampling'}, axis=1)
                ),
            output_format='TOA5'
            )
        data = io.reformat_data(
            data=self.make_dataframe(),
            output_format='TOA5'
            )
        io.write_data_to_file(
            headers=headers,
            data=data,
            abs_file_path=file_path,
            output_format='TOA5'
            )

    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------



###############################################################################
### BEGIN NC WRITERS ###
###############################################################################

#------------------------------------------------------------------------------
def make_nc_file(site: str, split_by_year: bool=True):
    """
    Merge all data and metadata sources to create a netcdf file.

    Args:
        site: name of site.
        split_by_year (optional): write discrete year files. Defaults to True.

    Returns:
        None.

    """

    data_builder = L1DataBuilder(site=site, concat_files=True)
    if not split_by_year:
        ds = data_builder.build_xarray_dataset_complete()
        output_path = data_builder.md_mngr.data_path / f'{site}_L1.nc'
        ds.to_netcdf(path=output_path, mode='w')
        return
    for year in data_builder.data_years:
        ds = data_builder.build_xarray_dataset_by_year(year=year)
        output_path = data_builder.md_mngr.data_path / f'{site}_{year}_L1.nc'
        ds.to_netcdf(path=output_path, mode='w')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_nc_year_file(site: str, year: int):
    """
    Merge all data and metadata sources to create a single year netcdf file.

    Args:
        site: name of site.
        year: year to write.

    Raises:
        IndexError: raised if dataset does not contain passed year.

    Returns:
        None.

    """

    data_builder = L1DataBuilder(site=site, concat_files=True)
    if not year in data_builder.data_years:
        raise IndexError('No data available for current data year!')
    ds = data_builder.build_xarray_dataset_by_year(year=year)
    output_path = data_builder.md_mngr.data_path / f'{site}_{year}_L1.nc'
    ds.to_netcdf(path=output_path, mode='w')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def append_to_current_nc_file(site: str):
    """
    Check for current year nc file, and if exists, append. If not, create.

    Args:
        site: name of site.

    Returns:
        None.

    """

    md_mngr = mh.MetaDataManager(site=site)
    expected_year = dt.datetime.now().year
    expected_file = md_mngr.data_path / f'{site}_{expected_year}_L1.nc'
    if not expected_file.exists():
        make_nc_year_file(site=site, year=expected_year)
    else:
        append_to_nc(site=site, nc_file=expected_file)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def append_to_nc(site: str, nc_file: pathlib.Path | str):
    """
    Generic append function. Only appends new data.

    Args:
        site: name of site.
        nc_file: nc file to which to append data.

    Returns:
        None.

    """

    ds = xr.open_dataset(nc_file)
    last_nc_date = pd.Timestamp(ds.time.values[-1]).to_pydatetime()
    md_mngr = mh.MetaDataManager(site=site)
    last_raw_date = min(
        [md_mngr.get_file_attrs(x)['end_date'] for x in md_mngr.list_files()]
        )
    if not last_raw_date > last_nc_date:
        print('No new data to append!')
        return
    data_builder = L1DataBuilder(site=site, md_mngr=md_mngr)
    date_iloc = data_builder.data.index.get_loc(last_nc_date) + 1
    new_ds = data_builder.build_xarray_dataset_by_slice(
        start_date=data_builder.data.index[date_iloc]
        )
    combined_ds = xr.concat([ds, new_ds], dim='time', combine_attrs='override')
    ds.close()
    combined_ds.attrs['time_coverage_end'] = (
        pd.to_datetime(combined_ds.time.values[-1])
        .strftime(TIME_FORMAT)
        )
    combined_ds.attrs['nc_nrecs'] = len(combined_ds.time)
    combined_ds.to_netcdf(path=nc_file, mode='w')
#------------------------------------------------------------------------------

###############################################################################
### END NC WRITERS ###
###############################################################################



###############################################################################
### BEGIN MERGE FUNCTIONS ###
###############################################################################

# #------------------------------------------------------------------------------
# def merge_data_by_manager(
#         site: str, md_mngr: mh.MetaDataManager=None, concat_files=False
#         ) -> pd.core.frame.DataFrame:
#     """


#     Args:
#         site (str): DESCRIPTION.
#         md_mngr (mh.MetaDataManager, optional): DESCRIPTION. Defaults to None.

#     Returns:
#         TYPE: DESCRIPTION.

#     """

#     if md_mngr is None:
#         md_mngr = mh.MetaDataManager(site=site)
#     merge_dict = {
#         file: md_mngr.translate_variables_by_table(table=table)
#         for table, file in md_mngr.map_tables_to_files(abs_path=True).items()
#         }
#     return fh.merge_data(files=merge_dict, concat_files=concat_files)
# #------------------------------------------------------------------------------

###############################################################################
### END MERGE FUNCTIONS ###
###############################################################################
