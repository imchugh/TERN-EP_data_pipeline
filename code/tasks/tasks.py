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
import yaml
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS
from data_constructors import data_constructors as datacon
from data_constructors import details_constructor as deetcon
from data_constructors import L1_workbook_constructor as xlcon
from file_handling import eddypro_concatenator as epc
from file_transfers import rclone_transfer as rt
from network_monitoring import network_status as ns
from paths import paths_manager as pm
#------------------------------------------------------------------------------

###############################################################################
### BEGIN TASK MANAGER ###
###############################################################################

# #------------------------------------------------------------------------------
# class TaskManager():

#     #--------------------------------------------------------------------------
#     def __init__(self):

#         self.configs = pm.get_local_config_file(config_stream='tasks')
#         self.site_master_list = self.configs['site_master_list']
#         self.master_tasks = list(self.configs['tasks'].keys())
#         self.network_tasks = [
#             key for key, value in self.configs['tasks'].items()
#             if value['type'] == 'network'
#             ]
#         self.site_tasks = [
#             key for key, value in self.configs['tasks'].items()
#             if value['type'] == 'site'
#             ]
#         self._make_df()
#         self._make_frequency_lists()
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def _make_df(self):

#         df = pd.DataFrame(
#             data=False,
#             index=self.site_master_list,
#             columns=self.master_tasks
#             )
#         for task in df.columns:
#             site_list = self.configs['tasks'][task]['site_list']
#             if site_list is None:
#                 site_list = self.site_master_list
#             df.loc[site_list, task] = True
#         self.df = df
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def _make_frequency_lists(self):

#         self.task_frequencies = {
#             'daily': [
#                 key for key, value in self.configs['tasks'].items()
#                 if value['frequency'] == 'daily'
#                 ],
#             '30min': [
#                 key for key, value in self.configs['tasks'].items()
#                 if value['frequency'] == '30min'
#                 ]
#             }
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_site_list_for_task(self, task):

#         return self.df[self.df[task]==True].index.tolist()
#     #--------------------------------------------------------------------------

#     #--------------------------------------------------------------------------
#     def get_task_list_for_site(self, site):

#         return self.df.loc[site][self.df.loc[site]].index.tolist()
#     #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

class TaskManager():
    
    #--------------------------------------------------------------------------
    def __init__(self):
    
        self.site_master_list = (
            pm.get_local_config_file(config_stream='site_master_list')
            ['site_master_list']
            )
        self.task_configs = pm.get_local_config_file(config_stream='tasks')
        self.master_tasks = list(self.task_configs.keys())
        self.tasks_df = self._make_task_dataframe()
        self.freqs_series = self._make_freq_series()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_task_dataframe(self) -> pd.DataFrame:

        tasks_df = pd.DataFrame(
            data=False,
            index=self.site_master_list,
            columns=self.master_tasks
            )
        for task in tasks_df.columns:
            site_list = self.task_configs[task]['site_list']
            if site_list is None:
                site_list = self.site_master_list
            tasks_df.loc[site_list, task] = True
        return tasks_df
    #--------------------------------------------------------------------------
        
    #--------------------------------------------------------------------------
    def _make_freq_series(self) -> pd.Series:
        
        keys = self.task_configs.keys()
        return pd.Series(
            data=[self.task_configs[key]['frequency'] for key in keys], 
            index=keys
            )       
    #--------------------------------------------------------------------------
   
    #--------------------------------------------------------------------------
    def get_site_list_for_task(self, task):

        return self.tasks_df[self.tasks_df[task]==True].index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_task_list_for_site(self, site):

        return self.tasks_df.loc[site][self.tasks_df.loc[site]].index.tolist()
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_task_list_by_frequency(self, frequency):
        
        return self.freqs_series.loc[self.freqs_series==frequency].index.tolist()
    #--------------------------------------------------------------------------   

    #--------------------------------------------------------------------------   
    def get_valid_task_frequencies(self):
        
        return self.freqs_series.unique().tolist()
    #-------------------------------------------------------------------------- 

#------------------------------------------------------------------------------

###############################################################################
### END TASK MANAGER ###
###############################################################################



#------------------------------------------------------------------------------
# INITS #
tasks_mngr = TaskManager()
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

#------------------------------------------------------------------------------
def generate_status_xlsx() -> None:
    
    ns.write_status_xlsx(
        site_list=tasks_mngr.get_site_list_for_task('generate_status_xlsx')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_status_geojson() -> None:
    
    ns.write_status_geojson(
        site_list=tasks_mngr.get_site_list_for_task('generate_status_geojson')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_status_geojson() -> None:

    _push_status_file(which='geojson')    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_status_xlsx() -> None:

    _push_status_file(which='xlsx')    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _push_status_file(which) -> None:

    resource = 'network'
    stream = f'status_{which}'
    rt.generic_move(
        local_location=pm.get_local_stream_path(
            resource=resource, stream=stream, as_str=True
            ),
        remote_location=pm.get_remote_stream_path(
            resource=resource, stream=stream, as_str=True
            ),
        which_way='to_remote',
        mod_time=False
        )
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
def get_task_function(task=None):
    """Map task string to task function."""

    funcs_dict = {

        # Data updates
        'generate_merged_file': generate_merged_file,
        'generate_L1_xlsx': xlcon.construct_L1_xlsx,
        'update_EddyPro_master': epc.update_eddypro_master,
        
        # Metadata updates
        'generate_site_details_file': deetcon.write_site_info,
        
        # Network status updates
        'generate_status_xlsx': generate_status_xlsx,
        'generate_status_geojson': generate_status_geojson,
        
        # Rclone transfers
        'Rclone_pull_slow_rdm': rt.pull_slow_flux,
        'Rclone_pull_RTMC_images': rt.pull_RTMC_images,
        'Rclone_push_RTMC_images': rt.push_RTMC_images,
        'Rclone_push_homogenised_TOA5': rt.push_homogenised_TOA5,
        'Rclone_push_status_geojson': push_status_geojson,
        'Rclone_push_status_xlsx': push_status_xlsx
        
        }

    if task is not None:
        return funcs_dict[task]
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
        function = get_task_function(task=task)
        function(site=site)
        logger.info('Task completed without error\n')
    except Exception:
        logger.error('Task failed with the following error:', exc_info=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_all_site_tasks(task: str) -> None:
    """
    Run a task for a single site (and log to single site log file).

    Args:
        task (TYPE): DESCRIPTION.
        site (TYPE): DESCRIPTION.

    Returns:
        None.

    """

    sites = tasks_mngr.get_site_list_for_task(task=task)
    for site in sites:
        run_site_task(task=task, site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_network_task(task):

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
    function = get_task_function(task=task)

    # Run the task
    logger.info(f'Running task {task}...')
    try:
        function()
        logger.info('Task completed without error\n')
    except Exception:
        logger.error('Task failed with the following error:', exc_info=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_task(task):

    if task not in tasks_mngr.master_tasks:
        raise KeyError(
            f'Task "{task}" not defined in configuration file!'
            )
    
    if task not in get_task_function():
        raise NotImplementedError(
            f'Function for task "{task}" not implemented!'
            )    

    task_func = get_task_function(task=task)
    args = list(inspect.signature(task_func).parameters.keys())
    if not args:
        run_network_task(task=task)
    elif args[0] == 'site':
        run_all_site_tasks(task=task)
    else:
        raise RuntimeError(f'Unknown task {task} passed!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_tasks_by_frequency(frequency: str) -> None:

    for task in tasks_mngr.get_task_list_by_frequency(frequency):
        try:
            run_task(task=task)
        except (KeyError, NotImplementedError):
            logger.error(f'Cannot execute task {task}: ', exc_info=True)
            continue
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_tasks_from_external_call(input_arg):
    
    if input_arg in tasks_mngr.get_valid_task_frequencies():
        run_tasks_by_frequency(frequency=input_arg)
    else:
        run_task(task=input_arg)
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def run_task(input_arg: str) -> None:
       
#     # if input_arg not in tasks_mngr.master_tasks:
#     #     raise KeyError(
#     #         f'Task "{input_arg}" not defined in configuration file!'
#     #         )
    
#     # if input_arg not in get_task_function():
#     #     raise NotImplementedError(
#     #         f'Function for task "{input_arg}" not implemented!'
#     #         )

#     if input_arg in tasks_mngr.get_valid_task_frequencies():
#         run_tasks_by_frequency(frequency=input_arg)
#     else:
#         run_task_from_list(task=input_arg)
    
    
    # except KeyError:
    #     raise NotImplementedError(f'Task "{input_arg}" not yet implemented!')
    # site_list = tasks_mngr.get_site_list_for_task()
    # args = list(inspect.signature(get_task_function(task)).parameters.keys())
#------------------------------------------------------------------------------

###############################################################################
### END TASK MANAGEMENT FUNCTIONS ###
###############################################################################

# if __name__=="__main__":
    
#     main()
    # main()    