#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 15:20:12 2026

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import datetime as dt
import pathlib
import subprocess

# -----------------------------------------------------------------------------

from managers import paths

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

# -----------------------------------------------------------------------------
def list_local_TOB3_files(site: str, abs_path: bool=True) -> list:
    """
    

    Args:
        site: name of site.
        abs_path (optional): Whether to return the absolute file path. 
        Defaults to True.

    Returns:
        list of locally-held files.

    """
    
    path = paths.get_local_stream_path(
        resource='raw_data', stream='flux_fast', site=site, subdirs=['TOB3']
        )
    gen = sorted(path.rglob('TOB3_*.dat'))
    if abs_path:
        return sorted(gen)
    return sorted([file.name for file in gen])
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def audit_TOB3_files(site: str, local: bool=True, freq: int=10) -> dict:
    """
    

    Args:
        site: name of site.
        local (optional): whether to check the local (True) or remote (False) 
        store. Defaults to True.
        freq (optional): 10Hz or 20Hz data frequency. Defaults to 10.

    Returns:
        dict containing missing but expected files and present but unexpected 
        files.

    """
    
    if local:
        existing_list = list_local_TOB3_files(site=site, abs_path=False)
    else:
        existing_list = list_remote_TOB3_files(site=site)

    expected_list = get_expected_files(
        site=site, 
        begin=parse_date(file_name=existing_list[0]), 
        end=parse_date(file_name=existing_list[-1]),
        freq=freq
        )
    
    return {
        'missing_but_expected': 
            sorted(set(expected_list) - set(existing_list)),
        'present_but_unexpected': 
            sorted(set(existing_list) - set(expected_list))
        }
# -----------------------------------------------------------------------------        

# -----------------------------------------------------------------------------        
def compare_local_remote_TOB3_files(site: str):
    """
    

    Args:
        site (TYPE): DESCRIPTION.

    Returns:
        None.

    """
    
    local_files = list_local_TOB3_files(site=site, abs_path=False)
    remote_files = list_remote_TOB3_files(site=site)
    return {
        'local_not_remote': sorted(set(local_files) - set(remote_files)),
        'remote_not_local': sorted(set(remote_files) - set(local_files))
        }
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def list_remote_TOB3_files(site: str) -> list:
    """
    

    Args:
        site (str): DESCRIPTION.

    Returns:
        list: DESCRIPTION.

    """
    
    remote_path = paths.get_remote_stream_path(
        resource='raw_data', 
        stream='flux_fast', 
        site='AliceMulga', 
        subdirs=['TOB3']
        )
    
    rslt = subprocess.run(
        ['rclone', 'lsf', '-R', remote_path, '--files-only'],
        capture_output=True,
        text=True,
        check=True
        ) 
    
    return [
        pathlib.Path(file).name for file in sorted(rslt.stdout.splitlines())
        ]
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def get_expected_files(
        site, begin: dt.datetime, end: dt.datetime, freq: int=10
        ) -> list:
    """
    

    Args:
        site (TYPE): DESCRIPTION.
        begin (dt.datetime): DESCRIPTION.
        end (dt.datetime): DESCRIPTION.
        freq (int, optional): DESCRIPTION. Defaults to 10.

    Raises:
        TypeError: DESCRIPTION.

    Returns:
        list: DESCRIPTION.

    """
    
    if not freq in [10, 20]:
        raise TypeError(
            '`freq` arg must be an integer and must be either 10 or 20'
            )
    
    interval = f'{int(1000 / freq)}ms'
    
    return [
        f'TOB3_{site}_{interval}_{day:%Y_%m_%d}.dat'
        for day in (
            begin + dt.timedelta(days=i) for i in range((end - begin).days)
            )
        ]
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def parse_date(file_name: str) -> dt.datetime:
    """
    

    Args:
        file_name (str): DESCRIPTION.

    Returns:
        TYPE: DESCRIPTION.

    """
    
    test = file_name.split('.')[0].split('_')
    return dt.datetime(int(test[-3]), int(test[-2]), int(test[-1]))
# -----------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################    