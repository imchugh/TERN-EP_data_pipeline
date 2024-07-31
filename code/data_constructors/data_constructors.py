# -*- coding: utf-8 -*-
"""
Created on Thu May 16 16:25:29 2024

@author: imchugh

This module is used to merge data from multiple raw data file sources into a
single file. The metadata required to find, collate and rename variables and
retrieve their attributes are accessed via the MetaDataManager class.

Module classes:
    - L1DataConstructor: builds an xarray dataset with data and metadata
    required to generate L1 .nc files.
    - NCConverter: used to extract data from an existing .nc file and push
    back out to Campbell Scientific TOA5 format data file.
    - StdDataConstructor: builds a pseudo-TOA5 dataset with a predictable
    variable set and standard naming conventions, with unit conversions and
    range-limits applied.

Module functions:
    - make_nc_file: generate the xarray dataset and write out to nc file.
    - make_nc_year_file: as above, but confined to a single data year.
    - append_to_current_nc_file: check for a current-year .nc file - append
    if it exists, generate it if it doesn't.
    - append_to_nc - generic append function where existing file is passed as
    arg.
    - merge_data: generic function to merge data using lower-level data
    handlers.

"""

import datetime as dt
import numpy as np
import pandas as pd
import pathlib
import xarray as xr

import data_constructors.convert_calc_filter as ccf
import utils.configs_manager as cm
import file_handling.file_io as io
import file_handling.file_handler as fh
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
MERGED_FILE_NAME = '<site>_merged_std.dat'
#------------------------------------------------------------------------------

###############################################################################
### BEGIN L1 DATA BUILDER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class L1DataConstructor():

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
            merge_data(
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
            self.md_mngr.get_site_details()
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
                dt.timedelta(minutes=self.global_attrs.time_step)
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

    data_builder = L1DataConstructor(site=site, concat_files=True)
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

    data_builder = L1DataConstructor(site=site, concat_files=True)
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
    data_builder = L1DataConstructor(site=site, md_mngr=md_mngr)
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
### BEGIN nc CONVERTER CLASS ###
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
### END nc CONVERTER CLASS ###
###############################################################################



###############################################################################
### BEGIN VISUALISATION DATA BUILDER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class StdDataConstructor():
    """Class to:
        1) merge variables from different sources;
        2) fill missing variables;
        3) apply broad range limits, and;
        4) output data to TOA5 file.
    """

    #--------------------------------------------------------------------------
    def __init__(
            self, site: str, include_missing=False, concat_files: bool=True
            ) -> None:
        """
        Assign metadata manager, missing variables and raw data and headers.

        Args:
            site: name of site.
            include_missing (optional): if True, accesses the
            requisite_variables yml for a list of minimum required variables,
            and attempts to calculate anything missing.
            concat_files (optional): whether to concatenate backups.
            Defaults to True.

        Returns:
            None.

        """

        # Set site and instance of metadata manager
        self.site = site
        self.md_mngr = AugmentedMetaDataManager(
            site=site, include_missing=include_missing
            )

        # Merge the raw data
        merge_dict = self.md_mngr.translate_variables_by_file(abs_path=True)
        rslt = merge_data(
            files=merge_dict,
            concat_files=concat_files,
            interval=f'{int(self.md_mngr.get_site_details().time_step)}T'
            )
        self.data = rslt['data']
        self.headers = rslt['headers']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def parse_data(self) -> pd.DataFrame:
        """
        Return a COPY of the raw data with QC applied (convert units,
        calculate requisite missing variables and apply range limits).

        Args:
            convert_units (optional): whether to convert units. Defaults to True.
            calculate_missing (optional): calculate missing variables. Defaults to True.
            apply_limits (optional): apply range limits. Defaults to True.


        Returns:
            output_data: the amended data.

        """

        output_data = self.data.copy()

        # Convert from site-based to standard units
        self._convert_units(df=output_data)

        # Calculate missing variables
        self._calculate_missing(df=output_data)

        # Apply range limits (if requested)
        self._apply_limits(df=output_data)

        return output_data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _convert_units(self, df: pd.DataFrame) -> None:
        """
        Apply unit conversions.

        Args:
            df: data for unit conversion.

        Returns:
            None.

        """

        for variable in self.md_mngr.list_variables_for_conversion():
            attrs = self.md_mngr.get_variable_attributes(variable)
            func = ccf.convert_variable(variable=attrs['quantity'])
            df[variable] = func(df[variable], from_units=attrs['units'])
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _calculate_missing(self, df: pd.DataFrame) -> None:
        """
        Calculate requisite missing quantities from existing data.

        Args:
            df: the data from which to source input variables.

        Returns:
            None.

        """


        for var in self.md_mngr.missing_variables.index:
            rslt = ccf.get_function(variable=var, with_params=True)
            args_dict = {
                parameter: df[parameter] for parameter in
                rslt[1]
                }
            df[var] = rslt[0](**args_dict)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _apply_limits(self, df: pd.DataFrame) -> None:
        """
        Apply range limits.

        Args:
            df: data to filter.

        Returns:
            None.

        """

        variables = pd.concat(
            [self.md_mngr.site_variables, self.md_mngr.missing_variables]
            )
        for variable in variables.index:
            attrs = variables.loc[variable]
            df[variable] = ccf.filter_range(
                series=df[variable],
                max_val=attrs.plausible_max,
                min_val=attrs.plausible_min
                )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def parse_headers(self) -> pd.DataFrame:
        """
        Return a COPY of the raw headers with converted units (if requested).

        Returns:
            headers.

        """

        output_headers = self.headers.copy()
        output_headers['units'] = (
            self.md_mngr.site_variables.loc[
                output_headers.index, 'standard_units'
                ]
            )
        if len(self.md_mngr.missing_variables) > 0:
            append_headers = (
                self.md_mngr.missing_variables[['standard_units']]
                .rename_axis('variable')
                .rename({'standard_units': 'units'}, axis=1)
                .assign(sampling='')
                )
            output_headers = pd.concat([output_headers, append_headers])
        return output_headers
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_to_file(self, path_to_file: pathlib.Path | str=None) -> None:
        """
        Write data to file.

        Args:
            path_to_file (optional): output file path. If None, slow data path
            and default name used. Defaults to None.

        Returns:
            None.

        """

        if not path_to_file:
            path_to_file = (
                self.md_mngr.data_path / f'{self.site}_merged.dat'
                )
        headers = self.parse_headers()
        data = self.parse_data()
        io.write_data_to_file(
            headers=io.reformat_headers(headers=headers, output_format='TOA5'),
            data=io.reformat_data(data=data, output_format='TOA5'),
            abs_file_path=path_to_file,
            output_format='TOA5'
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class AugmentedMetaDataManager(mh.MetaDataManager):
    """Adds missing variables to standard MetaDataManager class"""

    #--------------------------------------------------------------------------
    def __init__(self, site, include_missing):

        super().__init__(site, variable_map='vis')

        # Get missing variables and create table
        if include_missing:
            requisite_variables = (
                cm.get_global_configs(which='requisite_variables')['vis']
                )
            missing = [
                variable for variable in requisite_variables if not
                variable in self.site_variables.quantity.unique()
                ]
            if len(missing) > 0:
                self.missing_variables = self.standard_variables.loc[missing]
            else:
                self.missing_variables = pd.DataFrame()
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END VISUALISATION DATA BUILDER CLASS ###
###############################################################################



###############################################################################
### BEGIN VISUALISATION FILE WRITE FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def write_to_std_file(site: str, concat_files: bool=True) -> None:
    """
    Write data to file.

    Args:
        site: name of site.
        concat_files (optional): if True, concatenates all legal backups.
        Defaults to True.

    Returns:
        None.

    """

    # Get information for raw data
    data_const = StdDataConstructor(
        site=site, include_missing=True, concat_files=concat_files
        )

    # Get path information from the embedded metadata manager
    file_path = f'E:/Scratch/{site}_merged_std.dat'
    # file_path = data_const.md_mngr.data_path / MERGED_FILE_NAME

    # Parse data and reformat to TOA5
    data = io.reformat_data(
        data=data_const.parse_data(),
        output_format='TOA5'
        )

    # Parse headers and reformat to TOA5
    headers = (
        io.reformat_headers(
            headers=data_const.parse_headers(),
            output_format='TOA5'
            )
        )

    # Write data to file
    io.write_data_to_file(
        headers=headers,
        data=data,
        abs_file_path=file_path,
        output_format='TOA5'
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def append_to_std_file(site: str) -> None:
    """
    Append new data to file.

    Args:
        site: name of site.

    Raises:
        RuntimeError: raised if header mismatch between new and existing data.

    Returns:
        None.

    """

    # Get information for raw data
    data_const = StdDataConstructor(
        site=site, include_missing=True, concat_files=False
        )

    # Get path information from the embedded metadata manager
    file_path = f'E:/Scratch/{site}_merged_std.dat'
    # file_path = data_const.md_mngr.data_path / MERGED_FILE_NAME

    # Get the data and format as TOA5
    new_data = io.reformat_data(
        data=data_const.parse_data(),
        output_format='TOA5'
        )

    # Get information for existing standardised data record
    file_end_date = (
        io.get_start_end_dates(
            file=file_path,
            file_type='TOA5'
            )
        ['end_date']
        )

    # Check to see if any new data exists relative to existing file
    append_data = new_data.loc[file_end_date:].drop(file_end_date)
    if len(append_data) == 0:
        print('No new data to append')
        return

    # Cross-check header content
    existing_headers = io.get_header_df(file=file_path).reset_index()
    new_headers = (
        io.reformat_headers(
            headers=data_const.parse_headers(),
            output_format='TOA5'
            )
        .reset_index()
        )
    for column in existing_headers:
        try:
            assert all(new_headers==existing_headers)
        except AssertionError:
            raise RuntimeError(
                f'header row {column} in new data does not match header row '
                'in existing file!'
                )

    # Append to existing
    file_configs = cm.get_global_configs(which='file_formats')['TOA5']
    append_data.to_csv(
        path_or_buf=file_path, mode='a', header=False, index=False,
        na_rep=file_configs['na_values'], sep=file_configs['separator'],
        quoting=file_configs['quoting']
        )
#------------------------------------------------------------------------------

###############################################################################
### END VISUALISATION FILE WRITE FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN GENERIC FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def merge_data(
        files: list | dict, concat_files: bool=False, interval=None
        ) -> pd.core.frame.DataFrame:
    """
    Merge and align data and headers from different files.

    Args:
        files: the absolute path of the files to parse.
        If a list, all variables returned; if a dict, file is value, and key
            is passed to the file_handler. That key can be a list of variables, or
            a dictionary mapping translation of variable names (see file handler
                                                                documentation).
        concat_files (optional): concat backup files to current.
            Defaults to False
        interval (optional): resample files to passed interval.

    Returns:
        merged data.

    """

    data_list, header_list = [], []
    for file in files:
        try:
            usecols = files[file]
        except TypeError:
            usecols = None
        data_handler = fh.DataHandler(file=file, concat_files=concat_files)
        data_list.append(
            data_handler.get_conditioned_data(
                usecols=usecols,
                drop_non_numeric=True,
                monotonic_index=True,
                resample_intvl=interval
                )
            )
        header_list.append(
            data_handler.get_conditioned_headers(
                usecols=usecols, drop_non_numeric=True
                )
            )
    return {
        'headers': pd.concat(header_list),
        'data': pd.concat(data_list, axis=1)
        }
#------------------------------------------------------------------------------

###############################################################################
### END GENERIC FUNCTIONS ###
###############################################################################
