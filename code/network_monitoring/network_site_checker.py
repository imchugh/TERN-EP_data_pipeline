# -*- coding: utf-8 -*-
"""
Created on Mon Aug 26 12:26:03 2024

@author: jcutern-imchugh
"""

import yaml

from utils.configs_manager import PathsManager

paths = PathsManager()

#------------------------------------------------------------------------------
def _get_generic_configs(file):
    """


    Args:
        file (TYPE): DESCRIPTION.

    Returns:
        TYPE: DESCRIPTION.

    """

    with open(file) as f:
        return yaml.safe_load(stream=f)
#------------------------------------------------------------------------------

