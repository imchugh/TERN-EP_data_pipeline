# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 14:42:54 2024

@author: jcutern-imchugh
"""

import datetime as dt
import pandas as pd

import file_handling.file_io as io
import utils.metadata_handlers as mh


def get_data_status(site: str) -> pd.DataFrame():

    data = io.get_data(file='E:/Scratch/test.dat')
    l = []
    now = dt.datetime.now()
    for var in data.columns:
        s = data[var].dropna()
        lvr = s.index[-1]
        how_old = (now - lvr).days
        pct_valid = 0
        if not how_old > 0:
            pct_valid = round(
                len(s.loc[now - dt.timedelta(days=1): now]) /
                len(data[var].loc[now - dt.timedelta(days=1): now]) *
                100
                )
        l.append(
            {
                'last_valid_record': lvr,
                'days_since_last_valid_record': how_old,
                'last_24hr_pct_valid': pct_valid
                }
            )
    return pd.DataFrame(data=l, index=data.columns)



# class SiteDataStatusParser():

#     def __init__(self, site):

#         self.site = site
#         data_const = dtc.StdDataConstructor(
#             site=site, include_missing=False, concat_files=False
#             )
#         self.md_mngr = data_const.md_mngr
#         self.data = data_const.parse_data()

#     def get_logger_status(self):

#         files_df = (
#             self.md_mngr.site_variables[['logger', 'table', 'file']]
#             .drop_duplicates()
#             .reset_index(drop=True)
#             )
#         attrs_df = (
#             pd.concat(
#                 [self.md_mngr.get_file_attributes(x) for x in files_df.file],
#                 axis=1
#                 )
#             .T
#             .drop('table_name', axis=1)
#             )
#         attrs_df['days_since_last_record'] = (
#             (dt.datetime.now() - attrs_df.end_date)
#             .apply(lambda x: x.days)
#             )
#         return (
#             pd.concat([files_df, attrs_df], axis=1)
#             .set_index(keys=['logger', 'table'])
#             )

#     def get_data_status(self):

#         l = []
#         now = dt.datetime.now()
#         for var in self.data.columns:
#             s = self.data[var].dropna()
#             lvr = s.index[-1]
#             how_old = (now - lvr).days
#             pct_valid = 0
#             if not how_old > 0:
#                 pct_valid = round(
#                     len(s.loc[now - dt.timedelta(days=1): now]) /
#                     len(self.data[var].loc[now - dt.timedelta(days=1): now]) *
#                     100
#                     )
#             l.append(
#                 {
#                     'last_valid_record': lvr,
#                     'days_since_last_valid_record': how_old,
#                     'last_24hr_pct_valid': pct_valid
#                     }
#                 )
#         data_df = pd.DataFrame(data=l, index=self.data.columns)
#         metadata_df = self.md_mngr.site_variables.loc[
#             self.data.columns, ['logger', 'table']
#             ]

#         return pd.concat([metadata_df, data_df], axis=1)

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

def get_file_data_status(file):

    data = io.get_data(file=file)
    l = []
    now = dt.datetime.now()
    for col in data.columns:
        s = data[col].dropna()
        lvr = s.index[-1]
        how_old = (now - lvr).days
        pct_valid = 0
        if not how_old > 0:
            pct_valid = round(
                len(s.loc[now - dt.timedelta(days=1): now]) /
                len(data[col].loc[now - dt.timedelta(days=1): now]) *
                100
                )
        l.append(
            {
                'last_valid_record': lvr,
                'days_since_last_valid_record': how_old,
                'last_24hr_pct_valid': pct_valid
                }
            )
    return pd.DataFrame(data=l, index=data.columns)
