#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 16:41:00 2020

@author: imchugh
"""

#------------------------------------------------------------------------------
### IMPORTS ###
#------------------------------------------------------------------------------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------
R = 8.3143
CO2_molar_mass = 44
VALID_SITES = [
    'Boyagin', 'CumberlandPlain', 'HowardSprings', 'Litchfield', 'Whroo',
    'Warra', 'WombatStateForest'
    ]
COLORS_DICT = {
    'rtmc': {
        'background_color': (30/255, 30/255, 30/255),
        'font_color': 'white'
        },
    'standard': {
        'background_color': 'white',
        'font_color': 'black'
        }
    }
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_profile_dataset(site):

    if site == 'Boyagin':
        from profile_processing import Boyagin_data_prep as data_prep
    elif site == 'CumberlandPlain':
        from profile_processing import CumberlandPlain_data_prep as data_prep
    elif site == 'HowardSprings':
        import HowardSprings as data_prep
    elif site == 'Litchfield':
        import Litchfield as data_prep
    elif site == 'Warra':
        import Warra as data_prep
    elif site == 'Whroo':
        import Whroo as data_prep
    elif site == 'WombatStateForest':
        import WSF as data_prep

    return data_prep.return_data()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

class profile():

    def __init__(self, ds, site='Unknown'):

        self.dataset = ds
        self.site = site

    def get_heights(self):

        """Get gas sampling intake array heights in m"""

        return list(self.dataset.Height.data)

    def get_layer_depths(self):

        """Get distance in metres between intakes"""

        heights = self.get_heights()
        return np.array(heights) - np.array([0] + heights[:-1])

    def _get_layer_names(self):

        """Get name suffixes for layers"""

        layer_elems = [
            str(int(x)) if int(x) == x else str(x)
            for x in [0] + self.get_heights()
            ]
        return [
            f'{layer_elems[i-1]}-{layer_elems[i]}m'
            for i in range(1, len(layer_elems))
            ]

    def get_CO2_density(self, as_df=False):

        """Calculate the density in mgCO2 m^-3 from ideal gas law"""

        CO2_const = R / CO2_molar_mass
        da = (
            self.dataset.P * 1000 /
            (CO2_const * (self.dataset.Tair + 273.15)) *
            self.dataset.CO2 / 10**3
            )
        da.name = 'CO2_density'
        if not as_df: return da
        return _get_dataframe(da)

    def get_CO2_density_as_layers(self, as_df=False):

        """Get the layer mean CO2 density (lowest layer is assumed to be
           constant between ground and lowermost intake, other layers are
           simple mean of upper and lower bounds of layer)"""

        density_da = self.get_CO2_density()
        da_list = []
        da_list.append(
            density_da.sel(Height=density_da.Height[0])
            .reset_coords('Height', drop=True)
            )
        for i in range(1, len(density_da.Height)):
            da_list.append(
                density_da.sel(Height=density_da.Height[i-1: i+1])
                .mean('Height')
                )
        layer_da = xr.concat(da_list, dim='Layer')
        layer_da['Layer'] = self._get_layer_names()
        layer_da = layer_da.transpose()
        if not as_df: return layer_da
        return _get_dataframe(layer_da)

    def get_delta_CO2_storage(self, as_df=False):

        """Get storage term"""

        layer_da = self.get_CO2_density_as_layers()
        layer_da = layer_da / 44 * 10**3 # Convert g m^-3 to umol m^-3
        diff_da = layer_da - layer_da.shift(Time=1) # Difference
        diff_da = diff_da / 1800 # Divide by time interval
        depth_scalar = xr.DataArray(self.get_layer_depths(), dims='Layer')
        depth_scalar['Layer'] = diff_da.Layer.data
        diff_da = diff_da * depth_scalar
        diff_da['Layer'] = ['dCO2s_{}'.format(x) for x in diff_da.Layer.data]
        diff_da.name = 'delta_CO2_storage'
        if not as_df: return diff_da
        return _get_dataframe(diff_da)

    def get_summed_delta_CO2_storage(self, as_df=False):

        """Get storage term summed over all layers"""

        da = self.get_delta_CO2_storage()
        if not as_df: return da.sum('Layer', skipna=False)
        return da.sum('Layer', skipna=False).to_dataframe()

    def plot_diel_storage_mean(
            self, output_to_file=None, open_window=True, rtmc_opt=False
            ):

        """Plot the diel mean"""

        # Organise the data
        df = self.get_delta_CO2_storage(as_df=True)
        df['dCO2s_sum'] = df.sum(axis=1, skipna=False)
        diel_df = df.groupby([df.index.hour, df.index.minute]).mean()
        diel_df.index = np.arange(len(diel_df)) / 2
        diel_df.index.name = 'Time'

        # Now plot
        if not open_window:
            plt.ioff()
        fig, ax = plt.subplots(1, 1, figsize = (12, 8))
        ax.set_xlim([0, 24])
        ax.set_xticks([0,4,8,12,16,20,24])
        ax.axhline(0, color='black', ls=':')
        colour_idx = np.linspace(0, 1, len(diel_df.columns[:-1]))
        labs = [x.split('_')[1] for x in diel_df.columns]
        for i, var in enumerate(diel_df.columns[:-1]):
            color = plt.cm.cool(colour_idx[i])
            ax.plot(diel_df[var], label = labs[i], color = color)
        ax.plot(diel_df[diel_df.columns[-1]], label = labs[-1],
                color='grey')

        ax.legend(loc=[0.65, 0.18], frameon=False, ncol=2)
        col_scheme = 'standard'
        if rtmc_opt:
            col_scheme = 'rtmc'
        self._set_plot_configs(
            fig=fig,
            ax=ax,
            which=col_scheme,
            xlabel='$Time$',
            ylabel='$S_c$ ($\mu mol$ $CO_2$ $m^{-2}$ $s^{-1}$)',
            legend_loc='lower right',
            title='$CO_2$ storage evolution by layer'
            )

        if output_to_file: plt.savefig(fname=output_to_file)
        plt.ion()

    def plot_time_series(self, output_to_file=None, open_window=True):

        """Plot the time series"""

        df = self.get_delta_CO2_storage(as_df=True)
        strip_vars_list = [var.split('_')[1] for var in df.columns]

        if not open_window:
            plt.ioff()
        fig, ax = plt.subplots(1, 1, figsize = (12, 8))
        fig.patch.set_facecolor('white')
        colour_idx = np.linspace(0, 1, len(df.columns))
        ax.tick_params(axis = 'x', labelsize = 14)
        ax.tick_params(axis = 'y', labelsize = 14)
        ax.set_xlabel('$Date$', fontsize = 18)
        ax.set_ylabel(
            '$S_c$ ($\mu mol$ $CO_2$ $m^{-2}$ $s^{-1}$)', fontsize = 18
            )
        ax.xaxis.set_ticks_position('bottom')
        ax.yaxis.set_ticks_position('left')
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        plt.plot(self.get_summed_delta_CO2_storage(as_df=True),
                 label = 'Total', color = 'grey')
        for i, var in enumerate(df.columns):
            color = plt.cm.cool(colour_idx[i])
            plt.plot(df[var], label = strip_vars_list[i], color = color)
        plt.legend(loc='lower left', frameon = False, ncol = 2)
        if output_to_file: plt.savefig(fname=output_to_file)
        plt.ion()

    def plot_vertical_evolution_mean(
            self, output_to_file=False, open_window=True, rtmc_opt=False
            ):

        # Get data into shape
        df = self.dataset.to_dataframe().unstack()['CO2']
        grp_df = df.groupby([df.index.hour, df.index.minute]).mean()
        grp_df.index = np.linspace(0, 23.5, 48)
        grp_df.columns.name = 'Height'
        transform_df = pd.concat([grp_df.loc[x] for x in np.linspace(0,21,8)], axis=1).T
        transform_df.index.name = 'Time'

        # Switch off plotting for background processing
        if not open_window:
            plt.ioff()

        # Plot data
        fig, ax = plt.subplots(1, 1, figsize = (10, 10))
        colour_idx = np.linspace(0, 1, len(transform_df))
        for i, time in enumerate(transform_df.index):
            color = plt.cm.jet(colour_idx[i])
            ax.plot(
                transform_df.loc[time], transform_df.columns,
                color=color, lw=2,
                label=f'{str(int(time)).zfill(2)}00')

        # Configure plot
        col_scheme = 'standard'
        if rtmc_opt:
            col_scheme = 'rtmc'
        title = '$CO_2$ time evolution by height'# 'CO$_2$ time evolution\/by\/height$'
        self._set_plot_configs(
            fig=fig,
            ax=ax,
            which=col_scheme,
            xlabel='$CO_2$ ($\mu mol/mol)$',
            ylabel='Height (m)',
            legend_loc='upper right',
            title=title
            )

        # Output options
        if output_to_file: plt.savefig(fname=output_to_file)
        plt.ion()

    def _set_plot_configs(self, fig, ax, which, **kwargs):

        detail_color = COLORS_DICT[which]['font_color']
        bkgrnd_color = COLORS_DICT[which]['background_color']
        fig.patch.set_facecolor(color=bkgrnd_color)
        ax.set_facecolor(color=bkgrnd_color)
        for axis in ['x', 'y']:
            ax.tick_params(axis=axis, color=detail_color, labelcolor=detail_color)
        for spine in ['right', 'top']:
            ax.spines[spine].set_visible(False)
        ax.set_xlabel(kwargs['xlabel'], fontsize=18, color=detail_color)
        ax.set_ylabel(kwargs['ylabel'], fontsize=18, color=detail_color)
        ax.spines['left'].set_color(detail_color)
        ax.spines['bottom'].set_color(detail_color)
        ax.legend(
            loc=kwargs['legend_loc'], frameon=False, labelcolor=detail_color
            )
        fig.suptitle(kwargs['title'], fontsize=20, color=detail_color)

    def write_to_csv(self, file_name):

        df = self.get_delta_CO2_storage(as_df=True)
        df['dCO2s_total'] = self.get_summed_delta_CO2_storage(as_df=True)
        df.to_csv(file_name, index_label='DateTime')

    def write_to_netcdf(self, file_path, attrs=None):

        df = self.get_delta_CO2_storage(as_df=True)
        df['dCO2s_total'] = self.get_summed_delta_CO2_storage(as_df=True)
        ds = df.to_xarray()
        ds.attrs = {'Site': self.site,
                    'Heights (m)': ', '.join([str(i) for i in
                                              self.get_heights()]),
                    'Layer depths (m)': ', '.join([str(i) for i in
                                                   self.get_layer_depths()])}
        if attrs: ds.attrs.update(attrs)
        ds.Time.encoding = {'units': 'days since 1800-01-01',
                            '_FillValue': None}
        ds.to_netcdf(file_path, format='NETCDF4')

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_dataframe(this_da):

    df = this_da.to_dataframe().unstack()
    df.columns = df.columns.droplevel(0)
    if df.columns.dtype == object:
        return df[this_da[this_da.dims[1]].data]
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def load_site_profile_processor(site):

    return profile(
        ds=get_site_profile_dataset(site=site),
        site=site
        )
#------------------------------------------------------------------------------