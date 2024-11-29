#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 11:03:58 2024

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
import logging
import numpy as np
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
from paths import paths_manager as pm
#------------------------------------------------------------------------------

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

logger = logging.getLogger(__name__)
SEARCH_STR = 'TOB3*'

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

def move_main_data(site: str) -> None:

    logger.info('Moving local fast data to month_year directories')    
    _move_data(site)
    
def move_aux_data(site: str) -> None:
    
    logger.info('Moving local auxiliary fast data to month_year directories')
    _move_data(site, aux=True)

def _move_data(site: str, aux: bool=False) -> None:
    """
    Main function to move data from TMP directory into month_year directories

    Args:
        site: name of site.
        aux (optional): selects data target as either main (aux=False) or 
        auxiliary (aux=True). Defaults to False.

    Returns:
        None.

    """

    # Get file path    
    stream='flux_fast'
    if aux:
        stream += '_aux'
    file_path = pm.get_local_stream_path(
        resource='raw_data', 
        stream=stream, 
        site=site, 
        subdirs=['TMP'],
        check_exists=True
        )

    # Get the list of files
    logger.info('Getting file listing...')
    files = sorted(list(file_path.glob(SEARCH_STR)))

    if len(files) > 0:

        # Get the unique directories and check they exist (make if not)
        logger.info('Creating directories ')
        dirs = ['_'.join(file.stem.split('_')[3:5]) for file in files]
        for this_dir in np.unique(dirs).tolist():
            target_dir = file_path.parent / this_dir 
            if not target_dir.exists():
                target_dir.mkdir()
                
        # Transfer the files
        logging.info('moving files:')
        for file, new_dir in zip(files, dirs):
            parents = file.parents
            target = file.parents[1] / new_dir / file.name
            logger.info(
                f'  - {file.name}: {parents[0]} -> {target.parent}'
                )
            file.replace(target=target)
            
    else:
        
        logger.info('No files to move!')
        
    logger.info('Done!')
    
###############################################################################
### END FUNCTIONS ###
###############################################################################
