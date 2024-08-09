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
    'AliceSpringsMulga', 'Boyagin', 'Calperum', 'Fletcherview', 'Gingin',
    'GreatWesternWoodlands', 'HowardSprings', 'Litchfield', 'MyallValeA',
    'MyallValeB', 'Ridgefield', 'SnowGum', 'SturtPlains', 'Wellington',
    'Yanco'
    ]
paths = cm.PathsManager()
LOGGER_CONFIGS = 'logger_configs.yml'
LOG_BYTE_LIMIT = 10**6

#------------------------------------------------------------------------------
class TasksManager():

    #--------------------------------------------------------------------------
    def __init__(self):

        file_path = paths.get_local_stream_path(
            resource='configs',
            stream='tasks'
            )
        with open(file_path) as f:
            task_configs = yaml.safe_load(stream=f)
        self.site_tasks = pd.DataFrame(task_configs['site_tasks']).T
        self.generic_tasks = task_configs['generic_tasks']
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_list_for_task(self, task):
        """
        Get the list of sites for which a given task is enabled.
        """

        return (
            self.site_tasks.loc[self.site_tasks[task]==True]
            .index
            .to_list()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_task_list_for_site(self, site):
        """
        Get the list of tasks enabled for a given site.
        """

        return (
            self.site_tasks.loc[site, self.site_tasks.loc[site]==True]
            .index
            .tolist()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_task_list(self):
        """
        Get the list of tasks.
        """

        return self.site_tasks.columns.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_generic_task_list(self):

        return self.generic_tasks
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_task(task, site=None):

    site_only = {'site': site}
    tasks_dict = {

        'generate_merged_file': {
            'func': write_std_data,
            'args': site_only
            },

        'do_blah': {
            'func': ns.throw_error,
            'args': None
            }

        }

    sub_dict = tasks_dict[task]
    if not sub_dict['args'] is None:
        sub_dict['func'](**sub_dict['args'])
    else:
        sub_dict['func']()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def configure_logger(log_path):

    logger = logging.getLogger(__name__)
    with open(LOGGER_CONFIGS) as f:
        rslt = yaml.safe_load(stream=f)
        rslt['handlers']['file']['filename'] = str(log_path)
        logging.config.dictConfig(rslt)
    return logger
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_std_data(site):

    try:
        dtc.append_to_std_file(site=site)
    except FileNotFoundError:
        dtc.write_to_std_file(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_site_task(task, site):

    log_path = (
        paths.get_local_stream_path(
            resource='logs',
            stream='site_logs',
            site=site
            ) /
        f'{site}_{task}_b.txt'
        )
    logger = configure_logger(log_path=log_path)
    logger.info(f'Running task "{task}" for site {site}')
    try:
        run_task(task=task, site=site)
        logger.info('Task completed without error\n')
    except Exception:
        logger.error(
            'Task failed with the following error:',
            exc_info=True
            )
        pass
    logger.handlers.clear()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_generic_task(task):

    log_path = (
        paths.get_local_stream_path(
            resource='logs',
            stream='generic_logs'
            ) /
        f'{task}.txt'
        )
    logger = configure_logger(log_path)
    logger.info(f'Running task "{task}"...')
    try:
        run_task(task=task)
    except RuntimeError:
        logger.error('Task failed with the following error:', exc_info=True)

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def main():

    # If site name was passed, run task.
    # If not, check if the task is a site task, and run task for all sites for
    #   which task is enabled.
    # Otherwise run generic task.

    tasks = TasksManager()

    site_list = None
    task = sys.argv[1]
    try:
        site_list = [sys.argv[2]]
    except IndexError:
        if task in tasks.get_site_task_list():
            site_list = tasks.get_site_list_for_task(task=task)
    if site_list is not None:
        for site in site_list:
            run_site_task(task=task, site=site)
        return
    run_generic_task(task)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
if __name__=='__main__':

    main()
#------------------------------------------------------------------------------

