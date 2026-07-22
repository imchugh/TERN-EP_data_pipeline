#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 10:54:46 2026

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import pandas as pd

from services.domain.config_loader import load_config_file_from_name
from infrastructure import file_io

###############################################################################
### END IMPORTS ###
###############################################################################


###############################################################################
### BEGIN INITS ###
###############################################################################

FILE_FORMATS = load_config_file_from_name(name='raw_file_format')
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

###############################################################################
### END INITS ###
###############################################################################


###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

# -----------------------------------------------------------------------------

def load_raw_data(file, file_format: str, drop_non_numeric=True):

    # Initialise dispatcher and get formatter
    DATE_FORMATTERS = {
        'TOA5': _TOA5_date_formatter,
        'EddyPro': _EddyPro_date_formatter,
        }
    formatter = DATE_FORMATTERS.get(file_format)
    
    # Get data
    df = file_io.read_csv_data(
        path=file, 
        file_format=FILE_FORMATS[file_format]
        )
    
    # Apply formatting
    df = formatter(df)

    # Drop non-numeric and return
    return _drop_non_numeric(df=df, file_format=file_format)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------    

def _TOA5_date_formatter(df):
    
    dttm = pd.to_datetime(
        df['TIMESTAMP'],
        format=DATE_FORMAT,
        errors="coerce"
        )
    return df.set_index(keys=pd.Index(data=dttm, name='DATETIME'))
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def _EddyPro_date_formatter(df):
    
    dttm = pd.to_datetime(
        df["date"] + " " + df["time"],
        format=DATE_FORMAT,
        errors="coerce"
        )
    return df.set_index(keys=pd.Index(data=dttm, name='DATETIME'))
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def _drop_non_numeric(df, file_format):
    
    cols_to_drop = FILE_FORMATS[file_format]['non_numeric_cols']
    return df.drop(columns=cols_to_drop, errors='ignore')
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def get_adapter(system_type):

    adapters = {
        "CSI": CSIAdapter(),
        "Licor": LicorAdapter()
    }

    return adapters[system_type]
# -----------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################


###############################################################################
### BEGIN CLASSES ###
###############################################################################

# -----------------------------------------------------------------------------

class BaseAdapter:

    def load(self, file):
        raise NotImplementedError
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

class CSIAdapter(BaseAdapter):

    def load(self, file):

        df = load_raw_data(file=file, file_format="TOA5")
        df = df[~df.index.duplicated(keep="last")]

        return df        
# -----------------------------------------------------------------------------    

# -----------------------------------------------------------------------------
    
class LicorAdapter(BaseAdapter):

    def load(self, file):

        df = load_raw_data(file=file, file_format="EddyPro")
        df = df[~df.index.duplicated(keep="last")]

        return df
# -----------------------------------------------------------------------------    

