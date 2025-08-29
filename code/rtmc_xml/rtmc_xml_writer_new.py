# -*- coding: utf-8 -*-
"""
Created on Fri Jul 29 14:13:29 2022

This script uses a bunch of custom modules to edit a template rtmc file and
write back the changes to a new file. It only works for sites that have had
standard output files written.

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
#------------------------------------------------------------------------------

from matplotlib.pyplot import cm
import matplotlib.colors as colors
import numpy as np
import pandas as pd
import pathlib
import sys

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
#------------------------------------------------------------------------------

from managers import paths
from managers import metadata
from rtmc_xml import rtmc_xml_parser as rxp

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

PATH_TO_TEMPLATE = pathlib.Path('/home/imchugh/Documents/Template_2024.rtmc2')
WIN_PATH_TO_DATA = 'C:\\Users\\imchugh\\OneDrive - The University of Melbourne\\Documents\\RTMC\\Site_data\\'
WIN_PATH_TO_SNAPSHOTS = 'C:\\Users\\imchugh\\OneDrive - The University of Melbourne\\Documents\\RTMC\\Snapshots\\'
WIN_PATH_TO_IMAGES = 'C:\\Users\\imchugh\\OneDrive - The University of Melbourne\\Documents\\RTMC\\Site_images\\'

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### STUFF ###
#------------------------------------------------------------------------------

parser = rxp.rtmc_parser(path=PATH_TO_TEMPLATE)
syntax_generator = rxp.RtmcSyntaxGenerator()

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def colour_getter(search_str, n_col=6):
    """
    Generator to create a set of formatted strings to specify successive
    colours for a line plot

    Parameters
    ----------
    long_name : str
        The long name of the variable as it appears in RGB_DICT.
    n_col : int
        Number of colours to yield

    Yields
    ------
    colour_string : str
        RTMC format: "RGBA(r,g,b,a)" where r, g, b are 0-255 integers and a is
        alpha (transparency) parameter (just set to 1).

    """

    SCHEME = {
        'Fg': {
            'colour': 'Set1', 'seq': False, 'min': None, 'max': None
            },
        'Ts': {
            'colour': 'hot', 'seq': True, 'min': 0.3, 'max': 0.6
            },
        'Sws': {
            'colour': 'winter', 'seq': True, 'min': 0, 'max': 1
            }
        }
    scheme = SCHEME[search_str]
    cmap = cm.get_cmap(name=scheme['colour'])
    if scheme['seq']:
        cmap = colors.LinearSegmentedColormap.from_list(
            f'trunc({cmap.name},{scheme["min"]:.2f},{scheme["max"]:.2f})',
            cmap(np.linspace(scheme['min'], scheme['max'], 100))
            )
        colours = [cmap(val) for val in np.linspace(0, 1, n_col)]
    else:
        colours = [cmap(i) for i in range(8)]
    for colour in colours:
        yield (
            'RGBA('
            f'{",".join((np.array(colour[:3])*255).astype(int).astype(str))},'
            '1)'
            )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def edit_MetaDataManager(mdm: metadata.MetaDataManager) -> None:

    var_types = {
        'soil_heat_flux': 'Fg',
        'soil_temperature': 'Ts',
        'soil_moisture': 'Sws'
        }

    df = mdm.site_variables

    rename_dict = (
        pd.concat(
            [
                df.loc[df.quantity==x, 'quantity'] for
                x in metadata.TURBULENT_FLUX_QUANTITIES
                ]
            )
        .to_dict()
        )
    df = df.rename(rename_dict)

    drop_list = []
    soil_vars = get_soil_vars(site=mdm.site)
    for var_type, var_list in soil_vars.items():
        identifier = var_types[var_type]
        all_list = df[df.quantity==identifier].index.tolist()
        drop_list += [var for var in all_list if not var in var_list]

    df = df.drop(drop_list)
    mdm.site_variables = df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_soil_vars(site: str) -> dict:

    return paths.get_internal_configs('soil_variables')[site]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def line_plot_parser(mdm, search_str, screen_name, component_name):

    # Get the plot editor and find the existing labels
    plot_editor = parser.get_editor_by_component_name(
        screen=screen_name, component_name=component_name,
        )

    # Find the existing and new labels - if there are more in the template file,
    # drop the extras
    old_labels = [
        x for x in plot_editor.get_trace_labels()
        if plot_editor.get_axis_by_label(x)=='left'
        ]
    new_labels = [x for x in mdm.list_variables() if search_str in x]

    if len(old_labels) > len(new_labels):
        drop_labels = old_labels[len(new_labels):]
        for this_label in drop_labels:
            plot_editor.drop_trace_element_by_label(label=this_label)
            old_labels.remove(this_label)

    # Get the palette generator
    palette = colour_getter(search_str=search_str, n_col=len(new_labels))

    # Now reconstruct the plot traces
    for i, new_label in enumerate(new_labels):
        colour = next(palette)
        calculation_str = f'"DataFile:merged.{new_label}"'
        try:
            elem_label = old_labels[i]
            plot_editor.set_trace_attributes_by_label(
                label=elem_label, calculation=calculation_str, rgb=colour,
                new_label=new_label, title=new_label)
        except IndexError:
            elem_label = new_labels[0]
            plot_editor.duplicate_trace_element_by_label(
                old_label=elem_label, new_label=new_label
                )
            plot_editor.set_trace_attributes_by_label(
                label=new_label, calculation=calculation_str, rgb=colour,
                title=new_label)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def do_file_config(mdm):

    # Get the data source editor and set the source path
    data_source_editor = parser.get_file_source_editor(source_type='data')
    data_source_editor.get_set_source_file(
        path=WIN_PATH_TO_DATA + f'{mdm.site}_merged_std.dat'
        )

    # Get the details source editor and set the source path
    details_source_editor = parser.get_file_source_editor(source_type='details')
    details_source_editor.get_set_source_file(
        path=WIN_PATH_TO_DATA + f'{mdm.site}_details.dat'
        )

    # Get the settings editor and change the data path for the snapshot output
    settings_editor = parser.get_basic_settings_editor()
    settings_editor.get_set_snapshot_destination(
        text=WIN_PATH_TO_SNAPSHOTS + f'{mdm.site}'
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def do_system_config(mdm):

    # Set the screen name
    SCREEN = 'System'
    component_aliases = {
        'utc_time': 'Segmented Time1',
        'Comm Status Alarm': 'Comm Status Alarm',
        'No Data Alarm': 'No Data Alarm',
        'contour_image': 'Image1',
        'tower_image': 'Image2',
        'IRGA_signal_chart': 'Time Series Chart1',
        'IRGA_signal_digital': 'Digital13'
         }

    # Change the UTC offset argument to the correct one for site
    # (using negative minutes as offset from site time as format)
    time_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name=component_aliases['utc_time']
        )

    time_editor.get_set_element_offset_text(
        text=str(int(mdm.site_details['UTC_offset']) * -60)
        )

    # Change the comm status alarm component calculation string


    try:
        temp_logger_name = mdm.get_variable_attributes(
            variable='Fco2', return_field='logger'
            )
        logger_name = f'{mdm.site}_{temp_logger_name}'

        comm_status_editor = parser.get_editor_by_component_name(
            screen=SCREEN, component_name=component_aliases['Comm Status Alarm']
            )

        comm_status_editor.get_set_element_calculation_text(
            text=syntax_generator.get_comm_status_string(logger_name=logger_name)
            )

    except KeyError:
        pass

    # Change the no data alarm component calculation string
    try:
        table_name = mdm.get_variable_attributes(
            variable='Fco2', return_field='table'
            )
        no_data_editor = parser.get_editor_by_component_name(
            screen=SCREEN, component_name=component_aliases['No Data Alarm']
            )
        no_data_editor.get_set_element_calculation_text(
            text=syntax_generator.get_no_data_status_string(
                logger_name=logger_name, table_name=table_name
                )
            )

    except KeyError:
        pass

    # Reconfigure the figure sources for:
    # Contour image...
    try:
        img_editor = parser.get_editor_by_component_name(
            screen=SCREEN, component_name=component_aliases['contour_image']
            )
        img_editor.get_set_element_ImgName(
            text=WIN_PATH_TO_IMAGES + f'{mdm.site}_contour.png'
            )
    except FileNotFoundError:
        print(f'No contour image found for {mdm.site}')

    # Site image...
    try:
        img_editor = parser.get_editor_by_component_name(
            screen=SCREEN, component_name=component_aliases['tower_image']
            )
        img_editor.get_set_element_ImgName(
            text=WIN_PATH_TO_IMAGES + f'{mdm.site}_tower.jpg'
            )
    except FileNotFoundError:
        print('No tower image found for {}'.format(site))

    # Reconfigure the signal diagnostic plot to use the correct IRGA signal type
    signal_str = syntax_generator.get_aliased_output(
        var_list=['Sig_IRGA']
        )
    line_plot_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name=component_aliases['IRGA_signal_chart']
        )
    line_plot_editor.get_set_trace_calculation_by_label(
        label='Signal', calculation_text=signal_str
        )

    # Reconfigure the signal digital output to use the correct IRGA signal type
    digital_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name=component_aliases['IRGA_signal_digital']
        )
    digital_editor.get_set_element_calculation_text(text=signal_str)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def do_turbulent_flux_config(mdm):

    SCREEN = 'Turbulent_flux'
    component_aliases = {
        'soil_moisture_chart': 'Time Series Chart2',
        'soil_moisture_digital': 'Digital4',
        'soil_moisture_status_bar': 'Basic Status Bar4',
        'soil_temperature_chart': 'Time Series Chart3',
        'soil_temperature_digital': 'Digital2',
        'soil_temperature_status_bar': 'Basic Status Bar'
        }

    # Get the soil moisture variables
    soil_moist_vars = [x for x in mdm.list_variables() if 'Sws' in x]

    # Reconfigure the mean soil water trace of the time series
    digital_moist_str = syntax_generator.get_aliased_output(
        var_list=soil_moist_vars
        )
    line_plot_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name=component_aliases['soil_moisture_chart']
        )
    line_plot_editor.get_set_trace_calculation_by_label(
        label='Sws', calculation_text=digital_moist_str
        )

    # Reconfigure the mean soil moisture digital display
    soil_moist_digital_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name=component_aliases['soil_moisture_digital']
        )
    soil_moist_digital_editor.get_set_element_calculation_text(
        text=digital_moist_str
        )

    # Reconfigure the mean soil moisture basic status bar
    soil_moist_StatusBar_editor = parser.get_editor_by_component_name(
        screen=SCREEN,
        component_name=component_aliases['soil_moisture_status_bar']
        )
    soil_moist_StatusBar_editor.get_set_pointer_calculation_text(
        text=syntax_generator.get_aliased_output(
            var_list=soil_moist_vars,
            scaled_to_range=True,
            start_cond='start_absolute'
            )
        )

    # Get the soil temperature variables
    soil_T_vars = [x for x in mdm.list_variables() if 'Ts' in x]
    soil_T_str = syntax_generator.get_aliased_output(soil_T_vars)

    # Reconfigure the mean soil temperature trace of the air / soil T and Fsd chart
    soil_T_line_plot_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name=component_aliases['soil_temperature_chart']
        )
    soil_T_line_plot_editor.get_set_trace_calculation_by_label(
        label='Tsoil', calculation_text=soil_T_str
        )

    # Reconfigure the mean soil temperature digital display
    soil_T_digital_editor = parser.get_editor_by_component_name(
        screen=SCREEN,
        component_name=component_aliases['soil_temperature_digital']
        )
    soil_T_digital_editor.get_set_element_calculation_text(
        text=soil_T_str
        )

    # Reconfigure the mean soil temperature basic status bar
    soil_T_StatusBar_editor = parser.get_editor_by_component_name(
        screen=SCREEN,
        component_name=component_aliases['soil_temperature_status_bar']
        )
    soil_T_StatusBar_editor.get_set_pointer_calculation_text(
        text=syntax_generator.get_aliased_output(
            var_list=soil_T_vars, scaled_to_range=True,
            start_cond='start_absolute'
            )
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def do_radiant_flux_config(mdm):

    SCREEN = 'Radiant_flux'
    component_aliases = {
        'avail_energy_chart': 'Time Series Chart2',
        'energy_balance_chart': 'Time Series Chart3'
        }

    # Get soil variables for available energy and energy balance calculation
    soil_HF_list = [x for x in mdm.list_variables() if 'Fg' in x]
    soil_T_list = [x for x in mdm.list_variables() if 'Ts' in x]

    # Reconfigure available energy plot
    avail_energy_plot_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name=component_aliases['avail_energy_chart']
        )
    avail_energy_plot_editor.get_set_trace_calculation_by_label(
        label='Avail',
        calculation_text=syntax_generator.get_available_energy(
            soil_HF_list=soil_HF_list, soil_T_list=soil_T_list
            )
        )

    # Reconfigure available energy digital
    avail_energy_digital_editor=parser.get_editor_by_component_name(
        screen='Radiant_flux', component_name='Digital10'
        )
    avail_energy_digital_editor.get_set_element_calculation_text(
        syntax_generator.get_available_energy(
            soil_HF_list=soil_HF_list, soil_T_list=soil_T_list
            )
        )

    # Reconfigure residual plot
    residual_plot_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name=component_aliases['energy_balance_chart']
        )
    residual_plot_editor.get_set_trace_calculation_by_label(
        label='Residual',
        calculation_text=syntax_generator.get_energy_balance_residual(
            soil_HF_list=soil_HF_list, soil_T_list=soil_T_list
            )
        )
    residual_plot_editor.get_set_trace_calculation_by_label(
        label='Rad',
        calculation_text=syntax_generator.get_net_radiation(cuml=True)
        )
    residual_plot_editor.get_set_trace_calculation_by_label(
        label='NonRad',
        calculation_text=syntax_generator.get_net_non_radiant_energy(
            soil_HF_list=soil_HF_list, soil_T_list=soil_T_list, cuml=True
            )
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def do_soil_config(mdm):

    SCREEN = 'Soil'
    component_aliases = {
        'soil_heat_flux_chart': 'Time Series Chart',
        'soil_temperature_chart': 'Time Series Chart1',
        'soil_moisture_chart': 'Time Series Chart2'
        }

    # Get soil variables for available energy and energy balance calculation
    soil_HF_list = [x for x in mdm.list_variables() if 'Fg' in x]
    soil_T_list = [x for x in mdm.list_variables() if 'Ts' in x]

    # Reconfigure the soil heat flux plot
    line_plot_parser(
        mdm=mdm,
        search_str='Fg',
        screen_name=SCREEN,
        component_name=component_aliases['soil_heat_flux_chart']
        )

    # Reconfigure the soil temperature plot
    line_plot_parser(
        mdm=mdm,
        search_str='Ts',
        screen_name=SCREEN,
        component_name=component_aliases['soil_temperature_chart']
        )

    # Reconfigure the soil moisture plot
    line_plot_parser(
        mdm=mdm,
        search_str='Sws',
        screen_name=SCREEN,
        component_name=component_aliases['soil_moisture_chart']
        )

    # Reconfigure the soil heat flux and storage average plot
    # Get the editor
    soil_plot_editor = parser.get_editor_by_component_name(
        screen=SCREEN, component_name='Time Series Chart3'
        )

    # Edit storage
    soil_plot_editor.set_trace_attributes_by_label(
        label='Gs_mean',
        calculation=syntax_generator.get_soil_heat_storage(
            soil_T_list=soil_T_list
            )
        )

    # Edit heat flux plates
    soil_plot_editor.set_trace_attributes_by_label(
        label='Gz_mean',
        calculation=syntax_generator.get_soil_heat_flux(
            soil_HF_list=soil_HF_list
            )
        )

    # Edit combination method
    soil_plot_editor.set_trace_attributes_by_label(
        label='G_mean',
        calculation=syntax_generator.get_corrected_soil_heat_flux(
            soil_HF_list=soil_HF_list, soil_T_list=soil_T_list)
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def main(site, file_name=None):

    # Get the RTMC XML parser and edit it to standardise the flux naming conventions
    mdm = metadata.MetaDataManager(site=site)
    edit_MetaDataManager(mdm=mdm)


    # Don't allow overwrite of original template file, or misnaming of extension
    if file_name:
        if file_name == PATH_TO_TEMPLATE.name:
            raise FileExistsError('Overwrite of template not allowed!')
        if not file_name.split('.')[-1] == 'rtmc2':
            raise RuntimeError('File name must have .rtmc2 suffix!')
        output_path = str(PATH_TO_TEMPLATE.parent / file_name)
    else:
        output_path = str(PATH_TO_TEMPLATE.parent / f'{site}_test.rtmc2')

    # Edit the XML elements for each screen to reflect site variables
    do_file_config(mdm=mdm)
    do_system_config(mdm=mdm)
    do_turbulent_flux_config(mdm=mdm)
    do_radiant_flux_config(mdm=mdm)
    do_soil_config(mdm=mdm)

    # Write altered content to a new file
    parser.write_to_file(file_name=output_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
if __name__=='__main__':

    site = sys.argv[1]
    main(
        site=site,
        file_name=f'{site}_test.rtmc2'
        )
