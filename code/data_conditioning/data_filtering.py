# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 12:35:51 2024

@author: jcutern-imchugh
"""

import numpy as np

class data_filter():

    def __init__(self, data, max_val, min_val):

        pass

def filter_data(series, max_val, min_val):

    if isinstance(max_val, (int, float)) and isinstance(min_val, (int, float)):
        return series.where((series <= max_val) & (series >= min_val), np.nan)
    if isinstance(max_val, (int, float)):
        return series.where(series <= max_val, np.nan)
    if isinstance(min_val, (int, float)):
        return series.where(series >= min_val, np.nan)
    return series


