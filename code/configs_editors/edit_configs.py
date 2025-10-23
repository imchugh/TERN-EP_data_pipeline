#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 23 12:26:56 2025

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import logging
import json
import pathlib

#------------------------------------------------------------------------------

from managers import paths
from configs_editors import pfp_configs_editor as pconf

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def edit_L2_configs(site):
    
    # Check the input configuration file path exists
    path_to_config = (
        pathlib.Path('/store/Config_files/Sites/PFP_L2') / 
        f'{site}.txt'
        )
    if not path_to_config.exists():
        raise FileNotFoundError('No configuration file to edit!')
    
    # Check a json correction file exists
    path_to_json = (
        pathlib.Path('/store/Config_files/Sites/PFP_L2/TMP') / 
        f'{site}.json'
        )
    if not path_to_json.exists():
        raise FileNotFoundError('No new_corrections to apply!')
    
    # Get the configs editor
    l2ce = pconf.L2ConfigsEditor(input_file=str(path_to_config))
    
    # Get the json snippet
    with open(path_to_json) as f:
        json_input = json.load(f)
    
    # Check the json content
    if not json_input['site'] == site:
        raise KeyError(
            f'Passed site name ({site}) does not match json file site name '
            f'{json_input["site"]}'
            )
    if not 'variables' in json_input.keys():
        raise KeyError('No variables provided in json input file!')
    
    # Iterate over variables
    for variable, corrections in json_input['variables'].items():
        logger.info(f'Applying corrections for variable {variable}')
        
        # Iterate over corrections
        for correction in corrections:
            
            # Do the date exclusions
            if correction == 'ExcludeDates':
                logger.info(
                    'Incorporating date exclusions into existing configuration '
                    'file...'
                    )
                datepair_list = corrections['ExcludeDates']
                for date_pair in datepair_list:
                    l2ce.set_date_exclusions(
                        variable=variable, 
                        first_date=date_pair[0], 
                        last_date=date_pair[1]
                        )
                logger.info('Done!')
                
            # Do the range limits
            if correction == 'RangeCheck':
                logger.info(
                    'Incorporating range limits into existing configuration '
                    'file...'
                    )
                rangecheck_list = corrections['RangeCheck']
                l2ce.set_variable_range(
                    variable=variable, 
                    lower=rangecheck_list[0], 
                    upper=rangecheck_list[1]
                    )
                
        
    logger.info('Finished applying corrections.')
    logger.info('Writing to output file.')
    l2ce.write_configs(
        path_to_config.parent / 
        f'{path_to_config.stem}_v2.txt'
        )
#------------------------------------------------------------------------------

