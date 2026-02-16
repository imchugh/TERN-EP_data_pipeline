#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 10 08:58:23 2026

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import datetime as dt
import json
import pandas as pd
import pathlib
from typing import Generator

# -----------------------------------------------------------------------------

from managers import paths_new as paths

###############################################################################
### END IMPORTS ###
###############################################################################


###############################################################################
### BEGIN INITS ###
###############################################################################

TASK_STR = 'task_site_result'
LOG_PATH = paths.get_local_stream_path(resource='logs', stream='network_logs')
LOG_LIST = [file.stem for file in LOG_PATH.glob('*.jsonl')]

###############################################################################
### END INITS ###
###############################################################################


# -----------------------------------------------------------------------------
def get_json_log(file: pathlib.Path | str,) -> Generator:
    """
    Read in the jsonl task log.

    Args:
        file: absolute path to file.

    Yields:
        generator of json elements.

    """
    
    with open(file) as f:
        
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
# -----------------------------------------------------------------------------            

# -----------------------------------------------------------------------------
def get_run_ids(records: list) -> set:
    """
    Find all run ids in the log.

    Args:
        records: list of json records.

    Returns:
        set of ids.

    """
    
    return {r["run_id"] for r in records if "run_id" in r}
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def get_latest_run_id(records):
    """
    

    Args:
        records (TYPE): DESCRIPTION.

    Returns:
        TYPE: DESCRIPTION.

    """
    
    run_ids = get_run_ids(records=records)
    return max(run_ids) if run_ids else None
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def filter_by_run_id(records, run_id):
    """
    

    Args:
        records (TYPE): DESCRIPTION.
        run_id (TYPE): DESCRIPTION.

    Returns:
        list: DESCRIPTION.

    """
    
    return [rec for rec in records if rec['run_id'] == run_id]
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def filter_by_log_level(records, level='INFO'):
    """
    

    Args:
        records (TYPE): DESCRIPTION.
        level (TYPE, optional): DESCRIPTION. Defaults to 'INFO'.

    Returns:
        list: DESCRIPTION.

    """
    
    return [rec for rec in records if rec['level'] == level]
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def get_task_completion_status(task: str, run_id: str | None=None) -> dict:
    """
    Get the task completion status.

    Args:
        task (TYPE): DESCRIPTION.
        run_id (optional): DESCRIPTION. Defaults to None.

    Returns:
        dict: DESCRIPTION.

    """
    
    recs = list(get_json_log(LOG_PATH / f'{task}.jsonl'))
    if run_id is None:
        run_id = get_latest_run_id(records=recs)
    sub_recs = filter_by_run_id(records=recs, run_id=run_id)
    start, end = None, None
    for rec in sub_recs:
        if 'task_start' in rec['message']:
            start = rec
            continue
        if 'task_end' in rec['message']:
            end = rec
            continue
        if start is not None and end is not None:
            break
    
    started = dt.datetime.fromisoformat(start['timestamp']).astimezone()
    ended = dt.datetime.fromisoformat(end['timestamp']).astimezone()
    
    duration = str((ended - started).seconds)
    
    return {
        'name': task,
        'start_time': started.strftime('%Y-%m-%d %H:%M:%S'),
        'end_time': ended.strftime('%Y-%m-%d %H:%M:%S'),
        'duration (s)': duration,
        'task_status': end['status']
        }
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def get_site_result_status(task, run_id=None):
    
    recs = list(get_json_log(LOG_PATH / f'{task}.jsonl'))
    if run_id is None:
        run_id = get_latest_run_id(records=recs)
    recs = filter_by_run_id(records=recs, run_id=run_id)
    recs = filter_by_log_level(records=recs)
    recs = [rec for rec in recs if rec['message'] == 'task_site_result']
    return (
        pd.DataFrame(recs)
        .drop(['timestamp', 'level'], axis=1)
        .set_index('site')
        )
# -----------------------------------------------------------------------------
