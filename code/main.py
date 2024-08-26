# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 09:43:07 2024

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
# STANDARD IMPORTS
import logging.config
import pathlib
import sys
import yaml
import pandas as pd
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS
import utils.configs_manager as cm
import data_constructors.data_constructors as dtc
import network_monitoring.network_status as ns
import file_handling.eddypro_concatenator as epc
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# INITS

# Paths manager
paths = cm.PathsManager()

# Load task configuration file
task_configs_path = paths.get_local_stream_path(
    resource='configs',
    stream='tasks'
    )
with open(task_configs_path) as f:
    _task_configs = yaml.safe_load(stream=f)
NETWORK_TASKS = _task_configs['generic_tasks']
SITE_TASKS = pd.DataFrame(_task_configs['site_tasks']).T

# Load local logger configuration file
configs_path = pathlib.Path(__file__).parent / 'logger_configs.yml'
with open(configs_path) as f:
    LOGGER_CONFIGS = yaml.safe_load(stream=f)
logger = logging.getLogger(__name__)
#------------------------------------------------------------------------------



###############################################################################
### START TASK MANAGEMENT FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def configure_logger_path(log_path):

    if logger.hasHandlers():
        logger.handlers.clear()
    new_configs = LOGGER_CONFIGS.copy()
    new_configs['handlers']['file']['filename'] = str(log_path)
    logging.config.dictConfig(new_configs)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_list_for_task(task):

    return SITE_TASKS[SITE_TASKS[task]].index.tolist()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_merged_file(site):

    try:
        dtc.append_to_std_file(site=site)
    except FileNotFoundError:
        dtc.write_to_std_file(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_site_task(site, task):

    # Define task functions in dict
    site_only = {'site': site}
    tasks_dict = {

        'generate_merged_file': {
            'func': generate_merged_file,
            'args': site_only
            },

        'update_EddyPro_master': {
            'func': epc.update_eddypro_master,
            'args': site_only
            }

        }

    # Get the log output path and configure the logger
    log_path = (
        paths.get_local_stream_path(
            resource='logs',
            stream='site_logs',
            site=site
            ) /
        f'{site}_{task}_b.log'
        )
    configure_logger_path(log_path=log_path)

    # Run the task
    logger.info(f'Running task {task}...')
    run_dict = tasks_dict[task]
    try:
        run_dict['func'](**run_dict['args'])
        logger.info('Task completed without error\n')
    except Exception:
        logger.error('Task failed with the following error:', exc_info=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_network_task(task):

    # Define task functions in dict
    tasks_dict = {

        'generate_status_xlsx': ns.write_status_xlsx,
        'generate_status_geojson': ns.write_status_geojson

        }

    # Get the log output path and configure the logger
    log_path = (
        paths.get_local_stream_path(
            resource='logs',
            stream='network_logs',
            ) /
        f'{task}.log'
        )
    configure_logger_path(log_path=log_path)

    # Run the task
    logger.info(f'Running task {task}...')
    try:
        tasks_dict[task]()
        logger.info('Task completed without error\n')
    except Exception:
        logger.error('Task failed with the following error:', exc_info=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_task(task, site=None):

    if task in SITE_TASKS.columns:
        if site is not None:
            site_list = [sys.argv[2]]
        else:
            site_list = get_site_list_for_task(task=task)
        for site in site_list:
            run_site_task(site=site, task=task)
    elif task in NETWORK_TASKS:
        run_network_task(task=task)
#------------------------------------------------------------------------------

###############################################################################
### END TASK MANAGEMENT FUNCTIONS ###
###############################################################################

# #------------------------------------------------------------------------------
# def main():
#     """
#     Main externally-called function - evaluates task as site-based or
#         generic and executes accordingly.
#     Returns:
#         None.

#     """

#     # Inits
#     task = sys.argv[1]
#     if task in SITE_TASKS.columns:
#         try:
#             site_list = [sys.argv[2]]
#         except IndexError:
#             site_list = get_site_list_for_task(task=task)
#         for site in site_list:
#             run_site_task(site=site, task=task)

#     elif task in NETWORK_TASKS:
#         run_network_task(task=task)
# #------------------------------------------------------------------------------



# #------------------------------------------------------------------------------
# if __name__=='__main__':

#     main()
#------------------------------------------------------------------------------
