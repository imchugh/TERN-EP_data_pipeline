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
import netCDF4
import numpy as np
import pandas as pd
import pathlib
import xarray as xr

#------------------------------------------------------------------------------

from data_constructors import convert_calc_filter as ccf
from managers import metadata as md
from managers import paths
import file_handling.file_handler as fh

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

SITE_DETAIL_ALIASES = {'elevation': 'altitude'}
SITE_DETAIL_SUBSET = [
    'fluxnet_id', 'latitude', 'longitude', 'elevation', 'time_step',
    'time_zone', 'canopy_height', 'tower_height', 'soil', 'vegetation'
    ]
GLOBAL_METADATA_SUBSET = [
    'time_coverage_start', 'time_coverage_end', 'irga_type', 'sonic_type']
VAR_METADATA_SUBSET = [
    'height', 'instrument', 'long_name', 'standard_name', 'statistic_type',
    'standard_units'
    ]
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
HIST_STR = 'instrument_history'
parser = md.PFPNameParser()
logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN L1 NETCDF DATA CONSTRUCTOR CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class L1DataConstructor():

    #--------------------------------------------------------------------------
    def __init__(
        self, site: str, use_alternate_configs: pathlib.Path | str=None,
        concat_files: bool=False, constrain_start_to_flux: bool=False,
        constrain_end_to_flux: bool=True, pad_humidity_vars=False
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
            site=site, use_alternate_configs=use_alternate_configs
            )
        data = (
            self._build_internal_data(args=locals())
            .pipe(self._do_unit_conversions)
            .pipe(self._do_diag_conversions)
            .pipe(self._do_variance_conversions)
            # .pipe(self._add_humidity_variables)
            )
        
        if pad_humidity_vars:
            data = self._add_humidity_variables(data=data)
            
        self.data = data

        # Amend metadata manager to account for variance conversions
        self._amend_metadata_manager()

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
        merge_dict = self.md_mngr.translate_variables_by_file(abs_path=True)
        merge_to_int = f'{self.md_mngr.site_details["time_step"]}min'
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

            # If a variance, call the variance wrapper function
            if variable in self.md_mngr.list_variance_variables():
                data[variable] = ccf.convert_variance(
                    variable=attrs['quantity'],
                    data=data[variable],
                    variance_units=attrs['units']
                    )

            # If a standard variable, call the function directly
            else:

                func = ccf.convert_variable(variable=attrs['quantity'])
                data[variable] = func(
                    data=data[variable], from_units=attrs['units']
                    )

        return data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _do_diag_conversions(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Convert diagnostic definition from expression of valid samples to
        invalid samples.

        Args:
            data: uncorrected input data.

        Returns:
            corrected output data.

        """

        # Apply diagnostic conversions
        n_samples = (
            self.md_mngr.site_details['freq_hz'] *
            self.md_mngr.site_details['time_step'] *
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
    def _do_variance_conversions(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Convert variances to standard deviations.

        Args:
            data: the unconverted input data.

        Returns:
            the converted output data.

        """
        rename_dict = {
            var: var.replace('Vr', 'Sd') for var in
            self.md_mngr.list_variance_variables()
            }
        for variable in rename_dict.keys():
            data[variable] = ccf.convert_variance_stdev(data=data[variable])
        return data.rename(rename_dict, axis=1)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _add_humidity_variables(self, data):
        """
        

        Args:
            data (TYPE): DESCRIPTION.

        Returns:
            data (TYPE): DESCRIPTION.

        """
               
        for Ta_var in [x for x in data if 'Ta' in x]:
            got, get = {}, {}
            for quant in ['RH', 'AH']:
                # got, get = {}, {}
                humid_var = Ta_var.replace('Ta', quant)
                if humid_var in data.columns:
                    got[quant] = humid_var
                else:
                    get[quant] = humid_var
            if len(got) == 1 and len (get) == 1:
                get_quant = next(iter(get))
                get_var = get[get_quant]
                got_quant = next(iter(got))
                got_var = got[got_quant]
                self._add_humidity_metadata(get_var=get_var, got_var=got_var)
                if get_quant == 'AH':
                    data[get_var] = ccf.calculate_AH_from_RH(
                        Ta=data[Ta_var], RH=data[got_var], ps=data['ps']
                        )
                if get_quant == 'RH':
                    data[get_var] = ccf.calculate_RH_from_AH(
                        Ta=data[Ta_var], AH=data[got_var], ps=data['ps']
                        )
        return data
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _add_humidity_metadata(self, get_var, got_var):
        """
        

        Args:
            get_var (TYPE): DESCRIPTION.
            got_var (TYPE): DESCRIPTION.

        Returns:
            None.

        """
        
        attrs = self.md_mngr.get_variable_attributes(variable=got_var).to_dict()
        new_attrs = parser.parse_variable_name(get_var)
        attrs['units'] = new_attrs['standard_units']
        for field in ['name', 'logger', 'table', 'file']:
            attrs[field] = ''
        attrs.update(new_attrs)
        attrs_df = (
            pd.DataFrame([attrs])
            .set_index(pd.Index([get_var]))
            .rename_axis(self.md_mngr.site_variables.index.name)
            .fillna('')
            )
        self.md_mngr.site_variables = pd.concat(
            [self.md_mngr.site_variables, attrs_df]
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _amend_metadata_manager(self) -> None:
        """
        Amend the metadata manager to convert any variances to standard
        deviations.

        Returns:
            None.

        """

        rename_dict = {
            var: var.replace('Vr', 'Sd') for var in
            self.md_mngr.list_variance_variables()
            }
        for variable in rename_dict.keys():
            self.md_mngr.site_variables.loc[variable] = (
                self.md_mngr.get_variable_attributes(
                    variable=variable,
                    variance_2_stdev=True
                    )
                )
        self.md_mngr.site_variables = (
            self.md_mngr.site_variables.rename(rename_dict)
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _get_site_global_attrs(self) -> dict:
        """
        Combine default and site-specific info to create preliminary global
        attrs.

        Returns:
            dictionary with field names and site values.

        """

        # Get generic global attributes
        global_attrs = paths.get_internal_configs('generic_global_attrs')
        new_dict = {
            'metadata_link':
                global_attrs['metadata_link'].replace('<site>', self.site),
            'site_name': self.site
            }
        global_attrs.update(new_dict)

        # Get site-specific global attributes
        site_specific_attrs = (
            {
                attr: self.md_mngr.site_details[attr]
                for attr in SITE_DETAIL_SUBSET
                } |
            {'system_type': self.md_mngr.system_type}
            )

        for old_name, new_name in SITE_DETAIL_ALIASES.items():
            site_specific_attrs[new_name] = site_specific_attrs.pop(old_name)

        # Get custom global site attributes (any shared keys are overriden)
        try:
            custom_attrs = (
                paths.get_internal_configs(config_name='site_custom_metadata')
                [self.site]
                )
        except KeyError:
            custom_attrs = {}

        # Combine and return
        return global_attrs | site_specific_attrs | custom_attrs
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
            time_step=self.md_mngr.site_details['time_step']
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
                dt.timedelta(minutes=self.md_mngr.site_details['time_step'])
                )
            .year
            .unique()
            )
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

        dim_attrs = paths.get_internal_configs('nc_dim_attrs')
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

        ds.time.encoding['units']='seconds since 1800-01-01 00:00:00.0'
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
            attrs = (
                self.md_mngr.get_variable_attributes(
                    variable=var,
                    variance_2_stdev=True
                    )
                [VAR_METADATA_SUBSET]
                .rename({'standard_units': 'units'})
                )
            final_attrs = {}
            for key, value in attrs.items():
                if isinstance(value, str):
                    if len(value) == 0:
                        continue
                final_attrs[key] = value
            ds[var].attrs = final_attrs
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

        dim_attrs = paths.get_internal_configs('nc_dim_attrs')
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



###############################################################################
### BEGIN L1 NETCDF DATA MERGE CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class NCMerger():
    """Class for merging multiple netcdf files"""

    #--------------------------------------------------------------------------
    def __init__(self, file_list: list, expected_interval: int=30) -> None:
        """
        Initialise by parsing input files and gathering requisite info.

        Args:
            file_list: list of files to be combined.

        Raises:
            FileNotFoundError: raised if file doesn't exist.

        Returns:
            None.

        """

        # Assign private attributes
        self._expected_interval = expected_interval
        self._TYPES_DICT = {
            'irga': 'irga_type', 'sonic': 'sonic_type', 'flux': 'combined_type'
            }

        # Get attributes and assign to dataframe
        attrs_to_get = [
            'time_coverage_start', 'time_coverage_end', 'irga_type',
            'sonic_type'
            ]
        rslt = (
            {attr: [] for attr in attrs_to_get} |
            {'combined_type': [], 'abs_path': []}
            )
        for file in file_list:
            full_path = pathlib.Path(file)
            if not full_path.exists():
                raise FileNotFoundError('File {file.name} does not exist!')
            rslt['abs_path'].append(full_path)
            attrs = get_nc_global_attrs(file_path=file)
            [rslt[attr].append(attrs[attr]) for attr in attrs_to_get]
            rslt['combined_type'].append(
                f'[{attrs["irga_type"]},{attrs["sonic_type"]}]'
                )
        df = pd.DataFrame(
            data=rslt,
            index=[file_path.name for file_path in rslt['abs_path']]
            )
        for var in ['time_coverage_start', 'time_coverage_end']:
            df[var] = pd.to_datetime(df[var])
        self.file_info = df.sort_values(var)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_global_instrument_output_str(self, instrument_type):

        col_str = self._TYPES_DICT[instrument_type]
        return '; '.join(self.file_info[col_str].unique())
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def merge_files(self) -> xr.Dataset:
        """
        Merge the netcdf files, using a custom merge function for attrs.

        Returns:
            xarray dataset containing the merged information.

        """

        # Merge files and handle variable attribute merging
        ds = xr.open_mfdataset(
            paths=self.file_info.abs_path, combine_attrs=self._merge_attrs
            )

        # Update global variables as required
        span = {
            'time_coverage_start': self.file_info.time_coverage_start.min(),
            'time_coverage_end': self.file_info.time_coverage_end.max()
            }
        for key, date in span.items():
            ds.attrs[key] = date
        for instrument in ['irga_type', 'sonic_type']:
            ds.attrs[instrument] = (
                self.get_global_instrument_output_str(
                    instrument_type=instrument.split('_')[0])
                )

        # Return
        return ds
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _merge_attrs(self, attrs_list: list, context: dict=None) -> dict:
        """
        Combine a list of attribute dictionaries into a single dictionary, and
        combine instrument histories.

        Args:
            attrs_list: list of variable attribute dictionaries.
            context (optional): not used. Defaults to None.

        Returns:
            a single attribute dictionary with amended instrument histories
            (where applicable).

        """

        # WARNING - seems the attributes are passed to the function in reverse!
        attrs_list.reverse()

        # Check whether attribute dicts are the same
        if all(attrs==attrs_list[0] for attrs in attrs_list):
            return attrs_list[-1]

        histories = []

        # Iterate over all passed attribute dictionaries
        for i, attrs in enumerate(attrs_list):

            # Some variables do not have an instrument (e.g. time)
            if not 'instrument' in attrs.keys():
                continue

            # Parse any existing histories
            for key, value in attrs.items():
                if HIST_STR in key:
                    histories.append(_parse_history_str(value))

            # Test whether instrument changed from one file to next; if so,
            # add to histories
            try:
                if attrs['instrument'] != attrs_list[i + 1]['instrument']:
                    start = self.file_info.iloc[i]['time_coverage_start']
                    end = self.file_info.iloc[i]['time_coverage_end']
                    if len(histories) != 0:
                        start = (
                            max(history['end'] for history in histories) +
                            dt.timedelta(minutes=self._expected_interval)
                            )
                    histories.append(
                        {
                            'instrument': attrs['instrument'],
                            'start': start,
                            'end': end
                            }
                        )
            except IndexError:
                pass

        # If there are no histories, just return the most recent attrs
        if len(histories) == 0:
            return attrs_list[-1]

        # Create a dataframe, and 1) test for range overlap and 2) combine any
        # concurrent records
        df = (
            pd.DataFrame(histories)
            .sort_values(by='end')
            .pipe(_check_range_overlap)
            .pipe(_combine_concurrent)
            )

        # Add the history string to the dataframe index
        df.index = [f'{HIST_STR}_{i}' for i in range(len(df))]

        # Select the last attribute dictionary, scrub it of all histories,
        # then add the combined histories and return
        return (
            {
                key: value for key, value in attrs_list[-1].items()
                if HIST_STR not in key
                } |
            (
                (
                    df.instrument + ',' +
                    df.start.dt.strftime(TIME_FORMAT) + ',' +
                    df.end.dt.strftime(TIME_FORMAT)
                    )
                .to_dict()
                )
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_history_str(history: str) -> dict:
    """
    Split instrument history string, check, format and return parts as dict.

    Args:
        history: the history string.

    Raises:
        RuntimeError: raised if wrong number of elements following split.

    Returns:
        formatted comnponents.

    """

    split_list = history.split(',')
    if not len(split_list) == 3:
        raise RuntimeError(
            'instrument_history attribute must consist of three '
            'comma-separated elements (instrument, start date, end date)'
            )
    return {
        'instrument': split_list[0].strip(),
        'start': _date_str_converter(date=split_list[1].strip()),
        'end': _date_str_converter(date=split_list[2].strip())
        }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _combine_concurrent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks for concurrent instrument histories that can be combined into one.

    Args:
        df: dataframe containing instrument history components.

    Returns:
        dataframe with any concurrents combined.

    """

    dupe = df[df.instrument==df.instrument.shift()]
    if len(dupe) == 0:
        return df
    for row in dupe.index:
        df.loc[row - 1, 'end'] = dupe.loc[row, 'end']
        df = df.drop(row)
    return df.reset_index(drop=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _check_range_overlap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks all start and end dates valid (i.e. end after start) and do not
    overlap.

    Args:
        df: dataframe containing instrument history components (incl. dates).

    Raises:
        RuntimeError: raised if end before start or if dates for multiple
        histories overlap.

    Returns:
        unaltered dataframe.

    """

    for i in df.index:
        if not df.loc[i, 'start'] < df.loc[i, 'end']:
            raise RuntimeError(
                'Start date must be less than end date for instrument '
                f'{df.iloc[i, "instrument"]}'
                )
        if i > 0:
            if not df.loc[i, 'start'] > df.loc[i - 1, 'end']:
                raise RuntimeError(
                    f'Start date ({df.loc[i, "start"]}) for instrument '
                    f'{df.loc[i, "instrument"]} overlaps with end date '
                    f'({df.loc[i - 1, "end"]}) for instrument '
                    f'{df.loc[i - 1, "instrument"]}!'
                    )
    return df
#------------------------------------------------------------------------------

###############################################################################
### END L1 NETCDF DATA MERGE CLASS ###
###############################################################################

#------------------------------------------------------------------------------
def _date_str_converter(date: str | dt.datetime) -> str | dt.datetime:
    """
    Convert string to date or date to string.

    Args:
        date: date in either format.

    Returns:
        date in other format.

    """

    if isinstance(date, str):
        return dt.datetime.strptime(date, TIME_FORMAT)
    if isinstance(date, dt.datetime):
        return dt.datetime.strftime(date, TIME_FORMAT)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_nc_global_attrs(file_path: str | pathlib.Path) -> dict:
    """
    Extract the global attributes efficiently.

    Args:
        file_path: absolute path to file.

    Raises:
        FileNotFoundError: raised if file does not exist.

    Returns:
        dictionary of attrs.

    """

    if not pathlib.Path(file_path).exists():
        raise FileNotFoundError('File {file.name} does not exist!')
    with netCDF4.Dataset(file_path, 'r') as nc_file:
        return {
            attr: nc_file.getncattr(attr) for attr in nc_file.ncattrs()
            }
#------------------------------------------------------------------------------
