# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 09:43:07 2024

@author: jcutern-imchugh
"""



#------------------------------------------------------------------------------
# STANDARD IMPORTS
import inspect
import logging.config
import pandas as pd
import pathlib
import sys
import yaml
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS
import data_constructors.data_constructors as datacon
import data_constructors.details_constructor as deetcon
import network_monitoring.network_status as ns
import file_handling.eddypro_concatenator as epc
from paths import paths_manager as pm
#------------------------------------------------------------------------------

###############################################################################
### BEGIN TASK MANAGER ###
###############################################################################

#------------------------------------------------------------------------------
class TasksManager():

    #--------------------------------------------------------------------------
    def __init__(self):

        with open('/home/imchugh/Config_files/Tasks/tasks.yml') as f:
            self.configs=yaml.safe_load(f)
        self.site_master_list = self.configs['site_master_list']
        self.master_tasks = list(self.configs['tasks'].keys())
        self.network_tasks = [
            key for key, value in self.configs['tasks'].items()
            if value['type'] == 'network'
            ]
        self.site_tasks = [
            key for key, value in self.configs['tasks'].items()
            if value['type'] == 'site'
            ]
        self._make_df()
        self._make_frequency_lists()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_df(self):

        df = pd.DataFrame(
            data=False,
            index=self.site_master_list,
            columns=self.master_tasks
            )
        for task in df.columns:
            site_list = self.configs['tasks'][task]['site_list']
            if site_list is None:
                site_list = self.site_master_list
            df.loc[site_list, task] = True
        self.df = df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_frequency_lists(self):

        self.task_frequencies = {
            'daily': [
                key for key, value in self.configs['tasks'].items()
                if value['frequency'] == 'daily'
                ],
            '30min': [
                key for key, value in self.configs['tasks'].items()
                if value['frequency'] == '30min'
                ]
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_site_list_for_task(self, task):

        return self.df[self.df[task]==True].index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_task_list_for_site(self, site):

        return self.df.loc[site][self.df.loc[site]].index.tolist()
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END TASK MANAGER ###
###############################################################################



#------------------------------------------------------------------------------
# INITS #
tasks_mngr = TasksManager()
with open(pathlib.Path(__file__).parent / 'logger_configs.yml') as f:
    LOGGER_CONFIGS = yaml.safe_load(stream=f)
logger = logging.getLogger(__name__)
#------------------------------------------------------------------------------



###############################################################################
### BEGIN TASK DEFINITION FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def generate_merged_file(site: str) -> None:
    """Generates a TOA5 file for visualisation."""

    try:
        datacon.append_to_std_file(site=site)
    except FileNotFoundError:
        datacon.write_to_std_file(site=site)
#------------------------------------------------------------------------------

###############################################################################
### END TASK DEFINITION FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN TASK MANAGEMENT FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def configure_logger(log_path):
    """Configure the logger for the task (inclduing setting output path)."""
    
    if logger.hasHandlers():
        logger.handlers.clear()
    new_configs = LOGGER_CONFIGS.copy()
    new_configs['handlers']['file']['filename'] = str(log_path)
    logging.config.dictConfig(new_configs)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def retrieve_task_function(function=None):
    """Map task function to string."""

    funcs_dict = {

        'generate_merged_file': generate_merged_file,
        'update_EddyPro_master': epc.update_eddypro_master,
        'generate_status_xlsx': ns.write_status_xlsx,
        'generate_status_geojson': ns.write_status_geojson,
        'generate_site_details_file': deetcon.write_site_info

        }

    if function is not None:
        return funcs_dict[function]
    return list(funcs_dict.keys())
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_site_task(task: str, site:str) -> None:
    """
    Run a task for a single site (and log to single site log file).

    Args:
        task (TYPE): DESCRIPTION.
        site (TYPE): DESCRIPTION.

    Returns:
        None.

    """


    # Get the log output path and configure the logger
    log_path = (
        pm.get_local_stream_path(
            resource='logs',
            stream='site_logs',
            site=site
            ) /
        f'{site}_{task}.log'
        )
    configure_logger(log_path=log_path)      

    # Retrieve the function and run the task
    logger.info(f'Running task {task}...')
    try:
        function = retrieve_task_function(function=task)
        function(**{'site': site})
        logger.info('Task completed without error\n')
    except Exception:
        logger.error('Task failed with the following error:', exc_info=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_network_task(task, site_list):

    # Get the log output path and configure the logger
    log_path = (
        pm.get_local_stream_path(
            resource='logs',
            stream='network_logs',
            ) /
        f'{task}.log'
        )
    configure_logger(log_path=log_path)

    # Get the requested function
    function = retrieve_task_function(function=task)

    # Run the task
    logger.info(f'Running task {task}...')
    try:
        function(site_list=site_list)
        logger.info('Task completed without error\n')
    except Exception:
        logger.error('Task failed with the following error:', exc_info=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_task_from_list(task):

    site_list = tasks_mngr.get_site_list_for_task(task=task)
    if task in tasks_mngr.site_tasks:
        for site in site_list:
            run_site_task(site=site, task=task)
    elif task in tasks_mngr.network_tasks:
        run_network_task(task=task, site_list=site_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_tasks_by_frequency(frequency):

    for task in tasks_mngr.task_frequencies[frequency]:
        run_task_from_list(task=task)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def main():
    
    input_arg = sys.argv[1]
    
    if input_arg not in tasks_mngr.master_tasks:
        raise KeyError(
            f'Task "{input_arg}" not defined in configuration file!'
            )
    
    if input_arg not in retrieve_task_function():
        raise NotImplementedError(
            f'Function for task "{input_arg}" not implemented!'
            )

    if input_arg in tasks_mngr.task_frequencies.keys():
        run_tasks_by_frequency(frequency=input_arg)
    else:
        run_task_from_list(task=input_arg)
    
    
    # except KeyError:
    #     raise NotImplementedError(f'Task "{input_arg}" not yet implemented!')
    # site_list = tasks_mngr.get_site_list_for_task()
    # args = list(inspect.signature(retrieve_task_function(task)).parameters.keys())
#------------------------------------------------------------------------------

###############################################################################
### END TASK MANAGEMENT FUNCTIONS ###
###############################################################################

if __name__=="__main__":
    
    main()
    # main()    