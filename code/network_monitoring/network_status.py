# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 14:42:54 2024

@author: jcutern-imchugh
"""

import datetime as dt
import pandas as pd

import data_builders.data_merger as dm
import utils.metadata_handlers as mh


def get_site_logger_status(site):

    md_mngr = mh.MetaDataManager(site=site)
    files_df = (
        md_mngr.site_variables[['logger', 'table', 'file']]
        .drop_duplicates()
        .reset_index(drop=True)
        )
    attrs_df = (
        pd.concat(
            [md_mngr.get_file_attributes(x) for x in files_df.file],
            axis=1
            )
        .T
        .drop('table_name', axis=1)
        )
    attrs_df['days_since_last_record'] = (
        (dt.datetime.now() - attrs_df.end_date)
        .apply(lambda x: x.days)
        )
    return (
        pd.concat([files_df, attrs_df], axis=1)
        .set_index(keys=['logger', 'table'])
        )

def get_site_data_status(site):

    merger = dm.data_merger(site=site, variable_map='vis')
    data = merger.get_data(calculate_missing=False)
    l = []
    for col in data.columns:
        s = data[col].dropna()
        l.append({'last_valid_record': s.index[-1], 'value': s.iloc[-1]})
    data_df = pd.DataFrame(data=l, index=data.columns)
    data_df['days_since_last_valid_record'] = (
        data_df.last_valid_record.apply(lambda x: (dt.datetime.now() - x).days)
        )
    metadata_df = merger.md_mngr.site_variables.loc[
        merger.data.columns, ['logger', 'table']
        ]

    return pd.concat([metadata_df, data_df], axis=1)
