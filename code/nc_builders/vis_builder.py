# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 09:13:12 2024

@author: jcutern-imchugh
"""

import pathlib

from file_handling import file_handler as fh
from file_handling import file_io as io
from data_conditioning import data_filtering as dtf
from data_conditioning import data_convs_calcs as dtc
import utils.metadata_handlers as mh

def make_file(site, concat_files=True):

    # Get metadata manager
    md_mngr = mh.MetaDataManager(site=site, variable_map='vis')

    # Create a dict with file as key and variable translation dictionary as value
    merge_dict = {
        file: md_mngr.translate_variables_by_table(table=table)
        for table, file in md_mngr.map_tables_to_files(abs_path=True).items()
        }

    # Merge and rename the data from the different file sources
    data = fh.merge_data(files=merge_dict, concat_files=concat_files)

    # Iterate through variables
    for var in md_mngr.list_variables():

        # Get attributes
        var_attrs = md_mngr.get_variable_attributes(variable=var)

        # Convert from site-based to standard units (if different)
        if var_attrs['units'] != var_attrs['standard_units']:
            func = dtc.convert_variable(variable=var)
            data[var] = func(data[var], from_units=var_attrs['units'])

        # Apply range limits
        data[var] = dtf.filter_data(
            series=data[var],
            max_val=var_attrs['plausible_max'],
            min_val=var_attrs['plausible_min']
            )

    # Reformat data to TOA5
    data = io.reformat_data(data=data, output_format='TOA5')

    # return data
    return data

    # Get headers and reformat to TOA5
    headers = io.reformat_headers(
        headers=(
            fh.merge_headers(files=merge_dict, concat_files=concat_files)
            .rename({'statistic_type': 'sampling'}, axis=1)
            ),
        output_format='TOA5'
        )
    file_path = pathlib.Path('E:/Scratch') / 'Calperum_merged_std.dat'
    io.write_data_to_file(
        headers=headers,
        data=data,
        abs_file_path=file_path,
        output_format='TOA5'
        )

if __name__=='__main__':

    make_file(site='Calperum')