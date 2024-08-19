# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 09:43:07 2024

@author: jcutern-imchugh
"""

import logging
import pandas as pd
import sys
import yaml

import utils.configs_manager as cm
import data_constructors.data_constructors as dtc
import network_monitoring.network_status as ns

site_list = [
    'AliceSpringsMulga', 'Boyagin', 'Calperum', 'Fletcherview' 'Gingin',
    'GreatWesternWoodlands', 'HowardSprings', 'Litchfield', 'MyallValeA',
    'MyallValeB', 'Ridgefield', 'SnowGum', 'SturtPlains', 'Wellington',
    'Yanco'
    ]
paths = cm.PathsManager()

task_configs_path = paths.get_local_stream_path(
    resource='configs',
    stream='tasks'
    )
with open(task_configs_path) as f:
    _task_configs = yaml.safe_load(stream=f)
NETWORK_TASKS = _task_configs['generic_tasks']
SITE_TASKS = pd.DataFrame(_task_configs['site_tasks']).T

with open('logger_configs.yml') as f:
    LOGGER_CONFIGS = yaml.safe_load(stream=f)
logger = logging.getLogger(__name__)


# #------------------------------------------------------------------------------
# class TasksManager():

#     #--------------------------------------------------------------------------
#     def __init__(self):

#         file_path = paths.get_local_stream_path(
#             resource='configs',
#             stream='tasks'
#             )
#         with open(file_path) as f:
#             task_configs = yaml.safe_load(stream=f)
#         self.site_tasks = pd.DataFrame(task_configs['site_tasks']).T
#         self.generic_tasks = task_configs['generic_tasks']
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def classify_task(self, task):

#         if task in self.site_tasks:
#             return 'site_task'
#         if task in self.generic_tasks:
#             return 'network_task'
#         raise KeyError('Unrecognised task!')
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_site_log_path(self, task, site):

#         return (
#             paths.get_local_stream_path(
#                 resource='logs',
#                 stream='site_logs',
#                 site=site
#                 ) /
#             f'{site}_{task}.log'
#             )
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_generic_log_path(self, task):

#         return (
#             paths.get_local_stream_path(
#                 resource='logs',
#                 stream='generic_logs',
#                 ) /
#             f'{task}.log'
#             )
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_log_path(self, task, site=None):

#         task = self.classify_task(task=task)

#         if task == 'site_task':
#             if site is None:
#                 raise KeyError(
#                     'Site task was passed without site!'
#                     )
#             return (
#                 paths.get_local_stream_path(
#                     resource='logs',
#                     stream='site_logs',
#                     site=site
#                     ) /
#                 f'{site}_{task}_b.log'
#                 )


#         if task == 'network_task':
#             if site is not None:
#                 raise KeyError(
#                     'Passed task is a network task but site name was passed!'
#                     )
#             return (
#                 paths.get_local_stream_path(
#                     resource='logs',
#                     stream='network_logs',
#                     ) /
#                 f'{task}.log'
#                 )
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_site_list_for_task(self, task):
#         """
#         Get the list of sites for which a given task is enabled.
#         """

#         return (
#             self.site_tasks.loc[self.site_tasks[task]==True]
#             .index
#             .to_list()
#             )
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_task_list_for_site(self, site):
#         """
#         Get the list of tasks enabled for a given site.
#         """

#         return (
#             self.site_tasks.loc[site, self.site_tasks.loc[site]==True]
#             .index
#             .tolist()
#             )
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_site_task_list(self):
#         """
#         Get the list of tasks.
#         """

#         return self.site_tasks.columns.tolist()
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_generic_task_list(self):

#         return self.generic_tasks
#     #--------------------------------------------------------------------------

# #------------------------------------------------------------------------------

# tasks = TasksManager()

# #------------------------------------------------------------------------------
# def run_task(task, site=None):

#     site_only = {'site': site}
#     tasks_dict = {

#         'generate_merged_file': {
#             'func': write_std_data,
#             'args': site_only
#             },

#         'generate_status_xlsx': {
#             'func': ns.write_status_xlsx,
#             'args': None
#             }

#         }

#     try:

#         # Check the validity of passed arguments
#         log_path=tasks.get_log_path(task=task,site=site)

#         # Configure the logger and write info
#         configure_logger_path(log_path=log_path)
#         logger.info(f'Running task "{task}"...')

#         # Execute task
#         run_dict = tasks_dict[task]
#         if not run_dict['args'] is None:
#             run_dict['func'](**run_dict['args'])
#         else:
#             run_dict['func']()

#         logger.info('Task completed without error\n')

#     except Exception:

#         logger.error('Task failed with the following error:', exc_info=True)
# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def configure_logger_path(log_path):

    if logger.hasHandlers():
        logger.handlers.clear()
    new_configs = LOGGER_CONFIGS.copy()
    new_configs['handlers']['file']['filename'] = str(log_path)
    logging.config.dictConfig(new_configs)
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def run_dey_task(task, site=None):

#     configure_logger_path(log_path=tasks.get_log_path(task=task,site=site))
#     logger.info(f'Running task "{task}"...')
#     try:
#         run_task(task=task, site=site)
#         logger.info('Task completed without error\n')
#     except Exception:
#         logger.error('Task failed with the following error:', exc_info=True)
# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_list_for_task(task):

    return SITE_TASKS[SITE_TASKS['reformat_10Hz_main']].index.tolist()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_std_data(site):

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
            'func': write_std_data,
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

        'generate_status_xlsx': ns.write_status_xlsx

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
def main():
    """
    Main externally-called function - evaluates task as site-based or
        generic and executes accordingly.
    Returns:
        None.

    """

    # Inits
    task = sys.argv[1]
    if task in SITE_TASKS.columns:
        try:
            site_list = [sys.argv[2]]
        except IndexError:
            site_list = get_site_list_for_task(task=task)
        for site in site_list:
            run_site_task(site=site, task=task)

    elif task in NETWORK_TASKS:
        run_network_task(task=task)
#------------------------------------------------------------------------------


    # # If site-based task, check for site arg. If missing, get list from task
    # # manager
    # if task_type == 'site_task':
    #     try:
    #         site_list = [sys.argv[2]]
    #     except IndexError:
    #         site_list = tasks.get_site_list_for_task(task=task)
    #     for site in site_list:
    #         run_task(task=task, site=site)

    # # Otherwise execute generic task
    # else:
    #     run_task(task)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
if __name__=='__main__':

    main()
#------------------------------------------------------------------------------
