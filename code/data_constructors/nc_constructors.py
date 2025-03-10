# -*- coding: utf-8 -*-
"""
Created on Thu May 16 16:25:29 2024

@author: imchugh

This module is used to merge data from multiple raw data file sources into a
level 1 netcdf file. The metadata required to find, collate and rename
variables and retrieve their attributes are accessed via the MetaDataManager
class.

Module classes:
    - L1DataConstructor: builds an xarray dataset with data and metadata
    required to generate L1 .nc files.

"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import datetime as dt
import logging
import numpy as np
import pandas as pd
import pathlib
import xarray as xr

#------------------------------------------------------------------------------

from data_constructors import convert_calc_filter as ccf
from managers import metadata as md
from managers import paths
import file_handling.file_io as io
import file_handling.file_handler as fh

###############################################################################
### END IMPORTS ###
###############################################################################

#------------------------------------------------------------------------------
SITE_DETAIL_ALIASES = {'elevation': 'altitude'}
SITE_DETAIL_SUBSET = [
    'fluxnet_id', 'latitude', 'longitude', 'elevation', 'time_step',
    'time_zone', 'canopy_height', 'tower_height', 'soil', 'vegetation'
    ]
VAR_METADATA_SUBSET = [
    'height', 'instrument', 'long_name', 'standard_name', 'statistic_type',
    'standard_units'
    ]
STATISTIC_ALIASES = {'average': 'Avg', 'variance': 'Vr', 'sum': 'Tot'}
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logger = logging.getLogger(__name__)
#------------------------------------------------------------------------------

###############################################################################
### BEGIN L1 NETCDF DATA CONSTRUCTOR CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class L1DataConstructor():

    #--------------------------------------------------------------------------
    def __init__(
            self, site: str, use_alternate_configs: pathlib.Path | str=None,
            concat_files: bool=False, constrain_start_to_flux: bool=False,
            constrain_end_to_flux: bool=True
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
        self.md_mngr = md.MetaDataManager(
            site=site, use_alternate_configs=use_alternate_configs)
        self.data = (
            self._build_internal_data(args=locals())
            .pipe(self._do_unit_conversions)
            .pipe(self._do_diag_conversions)
            )

        # Set attributes
        self.data_years = self.data.index.year.unique().tolist()
        self.global_attrs = self._get_site_global_attrs()
        self.io_path = paths.get_local_stream_path(
            resource='homogenised_data', stream='nc', subdirs=[site]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _build_internal_data(self, args: dict) -> pd.DataFrame:

        # If requested, set flux file date constraints on merged file
        start_date, end_date = None, None
        if args['constrain_start_to_flux']:
            start_date = self.md_mngr.get_file_attributes(
                file=self.md_mngr.flux_file,
                include_backups=args['concat_files'],
                return_field='start_date'
                )
        if args['constrain_end_to_flux']:
            end_date = self.md_mngr.get_file_attributes(
                file=self.md_mngr.flux_file,
                include_backups=args['concat_files'],
                return_field='end_date'
                )

        # Merge the raw data
        merge_dict = {
            file: self.md_mngr.translate_variables_by_table(table=table)
            for table, file in self.md_mngr.map_tables_to_files(abs_path=True).items()
            }
        merge_to_int = f'{int(self.md_mngr.site_details.time_step)}min'
        return (
            fh.merge_data(
                files=merge_dict,
                concat_files=args['concat_files'],
                interval=merge_to_int,
                start_date=start_date,
                end_date=end_date
                )
            ['data']
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_unit_conversions(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Convert units from site-specific to network standard.

        Args:
            data: uncorrected input data.

        Returns:
            corrected output data.

        """

        # Apply unit conversions
        for variable in self.md_mngr.list_variables_for_conversion():
            attrs = self.md_mngr.get_variable_attributes(variable=variable)
            func = ccf.convert_variable(variable=attrs['quantity'])
            data[variable] = func(
                data=data[variable], from_units=attrs['units']
                )
        return data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_diag_conversions(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Converst diagnostic definition from expression of valid samples to
        invalid samples.

        Args:
            data: uncorrected input data.

        Returns:
            corrected output data.

        """

        # Apply diagnostic conversions
        n_samples = (
            self.md_mngr.site_details.freq_hz *
            self.md_mngr.site_details.time_step *
            60
            )
        for diag_var, units in self.md_mngr.diag_types.items():
            if units == 'valid_count':
                data[diag_var] = ccf.convert_diagnostic(
                    data=data[diag_var],
                    n_samples=n_samples,
                    from_units=units
                    )
        return data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_site_global_attrs(self) -> dict:
        """
        Combine default and site-specific info to create preliminary global
        attrs.

        Returns:
            dictionary with field names and site values.

        """

        global_attrs = io.read_yml(
            file=paths.get_local_stream_path(
                resource='configs',
                stream='nc_generic_attrs'
                )
            )
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
    def write_nc_file_by_year(
            self, year: int, output_path: pathlib.Path | str=None,
            overwrite: bool=False
            ):
        """


        Args:
            year (int): DESCRIPTION.
            output_path (pathlib.Path | str, optional): DESCRIPTION. Defaults to None.
            overwrite (bool, optional): DESCRIPTION. Defaults to False.

        Raises:
            FileExistsError: DESCRIPTION.

        Returns:
            None.

        """

        # Check paths
        if output_path is None:
            output_path = self.io_path / f'{self.site}_{year}_L1.nc'
        else:
            output_path = pathlib.Path(output_path)
        if output_path.exists():
            if not overwrite:
                raise FileExistsError('File already created for year {year}!')

        # Get data year and output to file
        (
            self.build_xarray_dataset_by_year(year=year)
            .to_netcdf(path=output_path)
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
                dt.timedelta(minutes=self.global_attrs['time_step'])
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

        dim_attrs = io.read_yml(
            file=paths.get_local_stream_path(
                resource='configs',
                stream='nc_dim_attrs'
                )
            )
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

        # Note 'standard units' is renamed to units because unit conversions
        # are applied to any variables that don't have standard units.
        for var in list(ds.keys()):
            ds[var].attrs = (
                self.md_mngr.get_variable_attributes(variable=var)
                [VAR_METADATA_SUBSET]
                .rename({'standard_units': 'units'})
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

        dim_attrs = io.read_yml(
            file=paths.get_local_stream_path(
                resource='configs',
                stream='nc_dim_attrs'
                )
            )
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
### END L1 NETCDF DATA CONSTRUCTOR CLASS ###
###############################################################################
