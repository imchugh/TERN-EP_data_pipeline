#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 10:58:21 2024

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
import logging
import pathlib
import subprocess as spc
#------------------------------------------------------------------------------

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

SCRIPT_PATH = pathlib.Path(__file__).parents[1] / 'shell/send_cosmoz_data.sh'
CSIRO_ALIASES = {
    'AliceSpringsMulga': 'AliceMulga', 'GreatWesternWoodlands': 'GWW'
    }
logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def push_cosmoz(site):

    try:
        remote_alias = CSIRO_ALIASES[site]
    except KeyError:
        remote_alias = site

    # Do the transfer
    logger.info('Copying now...')
    run_list =  [SCRIPT_PATH, site, remote_alias]
    try:
        rslt = _run_subprocess(run_list=run_list)
        logger.info(rslt.stdout.decode())
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logger.error(e)
        logger.error('Copy failed!')
        raise
    logger.info('Copy succeeded')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _run_subprocess(run_list, timeout=120):

    return spc.run(
        run_list, capture_output=True, timeout=timeout, check=True
        )
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
