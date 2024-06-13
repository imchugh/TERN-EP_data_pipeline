# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 16:09:49 2024

Todo:
    - Add the substring to raised exceptions so we can see if the request is mangled



"""

import datetime as dt
import json
import requests

import pandas as pd


###############################################################################
### BEGIN GLOBALS / CONSTANTS ###
###############################################################################

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
SECONDARY_TIME_FORMAT = TIME_FORMAT + '.%f'
ALLOWED_QUERY_MODES = [
    'most-recent', 'date-range', 'since-time', 'since-record', 'backfill'
    ]
VALID_FILE_SOURCES = ['CPU', 'CRD', 'USR']
VALID_FORMATS = ['html', 'json', 'toa5', 'tob1', 'xml']

###############################################################################
### END GLOBALS / CONSTANTS ###
###############################################################################



###############################################################################
### BEGIN CLASS SECTION ###
###############################################################################

#------------------------------------------------------------------------------
class LoggerDataManager():

    #--------------------------------------------------------------------------
    def __init__(self, IP_addr):

        self.table = build_lookup_table(IP_addr=IP_addr)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_variable_attributes(self, variable, return_field=None):

        if return_field is None:
            return self.table.loc[variable]
        return self.table.loc[variable, return_field]
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class CSILoggerMonitor():

    #--------------------------------------------------------------------------
    def __init__(self, IP_addr):

        self.IP_addr = IP_addr
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def clock_check(self):

        return clock_check(IP_addr=self.IP_addr)
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_by_date_range(
            self, start_date, end_date, table, variable=None
            ):

        return get_data_by_date_range(
            IP_addr=self.IP_addr, start_date=start_date, end_date=end_date,
            table=table, variable=variable
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_data_since_date(self, start_date, table, variable=None):

        return get_data_since_date(
            IP_addr=self.IP_addr, start_date=start_date, table=table,
            variable=variable
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END CLASS SECTION ###
###############################################################################



###############################################################################
### BEGIN MISCELLANEOUS FUNCTIONS SECTION ###
###############################################################################

#------------------------------------------------------------------------------
def clock_check(IP_addr: str) -> dict:
    """Check the logger clock.

    Args:
        IP_addr: IP address of the logger.

    Returns:
        Logger time.

    """

    cmd_str = build_cmd_str(IP_addr=IP_addr, cmd_substr='ClockCheck')
    return json.loads(do_request(cmd_str=cmd_str))
#------------------------------------------------------------------------------

###############################################################################
### END MISCELLANEOUS FUNCTIONS SECTION ###
###############################################################################



###############################################################################
### BEGIN DATA QUERY SECTION ###
###############################################################################

#------------------------------------------------------------------------------
def get_data_by_date_range(
        IP_addr: str, start_date: str | dt.datetime,
        end_date: str | dt.datetime, table: str, variable: str=None
        ) -> pd.DataFrame:
    """Get all of the data between specified start and end dates.

    Args:
        IP_addr: IP address of the device.
        start_date: start date.
        end_date: end date.
        table: table from which to collect data.
        variable (optional): the variable for which to collect data. Defaults to None.

    Returns:
        The data.

    """

    # Build the query substring
    cmd_substr = build_query_str(
        mode='date-range',
        config_str=(
            f'&p1={_convert_time_to_logger_format(time=start_date)}'
            f'&p2={_convert_time_to_logger_format(time=end_date)}'
            ),
        table=table,
        variable=variable
        )

    # Return data
    return _wrangle_data(
        IP_addr=IP_addr,
        cmd_substr=cmd_substr
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data_since_date(
        IP_addr: str, start_date: str | dt.datetime, table: str,
        variable: str=None
        ) -> pd.DataFrame:
    """Get all of the data after specified date.

    Args:
        IP_addr: IP address of the device.
        start_date: start date.
        table: table from which to collect data.
        variable (optional): the variable for which to collect data. Defaults to None.

    Returns:
        The data.

    """

    # Build the query substring
    cmd_substr = build_query_str(
        mode='since-time',
        config_str = (
            f'&p1={_convert_time_to_logger_format(time=start_date)}'
            ),
        table=table,
        variable=variable
        )

    # Return data
    return _wrangle_data(
        IP_addr=IP_addr,
        cmd_substr=cmd_substr
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data_n_records_back(
        IP_addr: str, table: str, recs_back: int=1, variable: str=None
        ) -> pd.DataFrame:
    """Get data starting n records back from present.

    Args:
        IP_addr: IP address of the device.
        table: table from which to collect data.
        recs_back: number of records to step back from present. Defaults to 1.
        variable (optional): the variable for which to collect data. Defaults to None.

    Returns:
        The data.

    """

    # Build the query substring
    cmd_substr = build_query_str(
        mode='most-recent',
        config_str=f'&p1={recs_back}',
        table=table,
        variable=variable
        )

    # Return data
    return _wrangle_data(
        IP_addr=IP_addr,
        cmd_substr=cmd_substr
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _wrangle_data(IP_addr: str, cmd_substr: str) -> pd.DataFrame:
    """Execute the command and shape the resulting data.

    Args:
        IP_addr: IP address of the device.
        cmd_substr: the substring to be embedded in the complete command string.

    Returns:
        The data.

    """

    cmd_str = build_cmd_str(IP_addr=IP_addr, cmd_substr=cmd_substr)
    content = json.loads(do_request(cmd_str=cmd_str))
    init_df = (
        pd.DataFrame(content['head']['fields'])
        .drop(['type', 'settable'], axis=1)
        .set_index(keys='name')
        .fillna('')
        )
    var_list = ['TIMESTAMP', 'RECORD'] + init_df.index.tolist()
    data_list = []
    for record in content['data']:
        time = _convert_time_from_logger_format(time_str=record['time'])
        record_n = int(record['no'])
        data_list.append([time, record_n] + record['vals'])
    return (
        pd.DataFrame(
            data=data_list, columns=var_list
            )
        .set_index(keys='TIMESTAMP')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_logger_info(IP_addr: str) -> dict:

    # Build the query substring
    cmd_substr = build_query_str(
        mode='most-recent',
        config_str='&p1=1',
        table='status',
        )


    cmd_str = build_cmd_str(IP_addr=IP_addr, cmd_substr=cmd_substr)
    content = json.loads(do_request(cmd_str=cmd_str))
    info_dict = content['head']['environment']
    info_dict.update({'prog_sig': content['head']['signature']})
    return content['head']['environment']
#------------------------------------------------------------------------------

###############################################################################
### END DATA QUERY SECTION ###
###############################################################################



###############################################################################
### BEGIN TABLE QUERY SECTION ###
###############################################################################

#------------------------------------------------------------------------------
def get_tables(IP_addr: str, list_only: bool=True) -> list | pd.DataFrame:
    """Get the list of tables available on the logger.

    Args:
        IP_addr: IP address of the device.
        list_only (optional): whether to return just a list of names, or all info. Default is True.
    Returns:
        The tables.

    """

    return _wrangle_tables(
        IP_addr=IP_addr,
        cmd_substr='browsesymbols&uri=dl:',
        list_only=list_only
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_table_variables(
        IP_addr: str, table: str, list_only: bool=True
        ) -> list | pd.DataFrame:
    """Get the list of variables available in a given table.

    Args:
        IP_addr: IP address of the device.
        table: the table for which to return the variables.
        list_only (optional): whether to return just a list of variables, or all info.
    Returns:
        The variables.

    """

    return _wrangle_tables(
        IP_addr=IP_addr,
        cmd_substr=f'browsesymbols&uri=dl:{table}',
        list_only=list_only
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_table_headers(IP_addr: str, table: str) -> pd.DataFrame:
    """Return the headers (units and process) of the table.

    Args:
        IP_addr: IP address of the device.
        table: the table for which to return the headers.

    Returns:
        The headers.

    """

    cmd_substr = build_query_str(
        mode='most-recent',
        config_str='&p1=1',
        table=table,
        )
    cmd_str = build_cmd_str(IP_addr=IP_addr, cmd_substr=cmd_substr)
    content = json.loads(do_request(cmd_str=cmd_str))
    return (
        pd.DataFrame(content['head']['fields'])
        .drop(['type', 'settable'], axis=1)
        .rename({'name': 'variable'}, axis=1)
        .set_index(keys='variable')
        .fillna('')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _wrangle_tables(
        IP_addr: str, cmd_substr: str, list_only: bool | pd.DataFrame
        ) -> pd.DataFrame():
    """Execute the command and shape the resulting data.

    Args:
        IP_addr: IP address of the device.
        cmd_substr: the substring to be embedded in the complete command string.
        list_only: whether to return a full dataframe or just a list.

    Returns:
        The tables.

    """

    cmd_str = build_cmd_str(IP_addr=IP_addr, cmd_substr=cmd_substr)
    rslt = json.loads(do_request(cmd_str=cmd_str))
    data = (
        pd.DataFrame(rslt['symbols'])
        .drop('type', axis=1)
        .set_index(keys='name')
        )
    if list_only:
        return data.index.tolist()
    return data
#------------------------------------------------------------------------------

###############################################################################
### END TABLE QUERY SECTION ###
###############################################################################



###############################################################################
### BEGIN FILE QUERY SECTION ###
###############################################################################

#------------------------------------------------------------------------------
def list_files(IP_addr: str, source: str) -> pd.DataFrame:
    """List the available files.

    Args:
        IP_addr: IP address of the device.
        source: the source to check (CPU, CRD or USR).

    Raises:
        FileNotFoundError: raised if the source is invalid.

    Returns:
        Files, including size and last write date and time.

    """

    drop_list = [
        'is_dir', 'run_now', 'run_on_power_up', 'read_only', 'paused'
        ]
    if not source in VALID_FILE_SOURCES:
        raise FileNotFoundError(f'{source} is not a valid file source!')
    cmd_str = build_cmd_str(
        IP_addr=IP_addr,
        cmd_substr='ListFiles',
        source=source
        )
    rslt = json.loads(do_request(cmd_str=cmd_str))
    df = pd.DataFrame(rslt['files'])
    df.path = df.path.str.replace(f'{source}/', '')
    df.last_write = df.last_write.apply(_convert_time_from_logger_format)
    return (
        df[~df.is_dir]
        .drop(drop_list, axis=1)
        .rename({'path': 'file'}, axis=1)
        .set_index(keys='file')
        )
#------------------------------------------------------------------------------

# def get_newest_file(IP_addr, file_ext: str=None) -> str:

#     cmd_str = f'http://{IP_addr}/?command=NewestFile&expr=CRD:*.dat'
#     rslt = do_request(cmd_str=cmd_str)

###############################################################################
### END FILE QUERY SECTION ###
###############################################################################



###############################################################################
### BEGIN QUERY STRING BUILDING SECTION ###
###############################################################################

#------------------------------------------------------------------------------
def do_request(cmd_str: str) -> dict:

    rslt = requests.get(cmd_str, stream=True, timeout=30)
    if not rslt.status_code == 200:
        raise RuntimeError(
            f'Request {cmd_str} failed with status code {rslt.status_code}!'
            )
    return rslt.content
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def build_cmd_str(IP_addr, cmd_substr, out_format='json', source=None):

    addr_syntax = f'http://{IP_addr}/'
    command_syntax = f'?command={cmd_substr}'

    source_syntax = ''
    if not source is None:
        if not source in VALID_FILE_SOURCES:
            raise KeyError(
                f'source must be one of {", ".join(VALID_FILE_SOURCES)}'
                )
        source_syntax = f'{source}/'

    format_syntax = ''
    if not out_format is None:
        if not out_format in VALID_FORMATS:
            raise KeyError(
                f'out_format must be one of {", ".join(VALID_FORMATS)}'
                )
        format_syntax = f'&format={out_format}'

    return ''.join([addr_syntax, source_syntax, command_syntax, format_syntax])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def build_query_str(table, mode, config_str, variable=None):

    variable_syntax = ''
    if not variable is None:
        variable_syntax = f'.{variable}'

    return f'dataquery&uri=dl:{table}{variable_syntax}&mode={mode}{config_str}'
#------------------------------------------------------------------------------

###############################################################################
### END QUERY STRING BUILDING SECTION ###
###############################################################################

def build_lookup_table(IP_addr):

    df_list = []
    for table in get_tables(IP_addr=IP_addr):
        df = get_table_headers(IP_addr=IP_addr, table=table)
        df['table'] = table
        df_list.append(df)
    return (
        pd.concat(df_list)
        [['units', 'process', 'table']]
        .fillna('')
        )

###############################################################################
### BEGIN PRIVATE FUNCTION SECTION ###
###############################################################################

def _convert_time_to_logger_format(time):

    if isinstance(time, str):
        time = dt.datetime.strptime(time, TIME_FORMAT)
    format_str = TIME_FORMAT.replace(' ', 'T')
    return dt.datetime.strftime(time, format_str)

def _convert_time_from_logger_format(time_str):

    eval_str = time_str.replace('T', ' ')
    try:
        return dt.datetime.strptime(eval_str, TIME_FORMAT)
    except ValueError as e:
        try:
            return dt.datetime.strptime(eval_str, SECONDARY_TIME_FORMAT)
        except ValueError:
            raise e

###############################################################################
### END PRIVATE FUNCTION SECTION ###
###############################################################################
