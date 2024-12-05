# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 09:43:07 2024

@author: jcutern-imchugh

Note that imports are embedded in the task function calls so that all modules 
do not need to be loaded every time the task manager is called externally!
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
from importlib import import_module
import inspect
import logging.config
import pandas as pd
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
# from data_constructors import data_constructors as datacon
# from data_constructors import details_constructor as deetcon
from data_constructors import L1_workbook_constructor as xlcon
from file_handling import eddypro_concatenator as epc
from file_transfers import rclone_transfer as rt
# from file_transfers import sftp_transfer as sftpt
# from network_monitoring import network_status as ns
from utils import configs_getters as cg
from paths import paths_manager as pm
#------------------------------------------------------------------------------

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN TASK MANAGER ###
###############################################################################

#------------------------------------------------------------------------------
class TaskManager():
    
    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        """
        Build yml task configuration file into a dataframe to interrogate
        (tasks x sites matrix).

        """
    
        self.configs = cg.get_configs(config_name='tasks')
        self.site_master_list = self.configs['sites']
        self.master_tasks = list(self.configs['tasks'].keys())
        self.tasks_df = self._make_task_dataframe()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_task_dataframe(self) -> pd.DataFrame:
        """Make a dataframe with tasks x sites matrix"""
        
        tasks_df = pd.DataFrame(
            data=False,
            index=self.site_master_list,
            columns=self.master_tasks
            )
        for task in tasks_df.columns:
            site_list = self.configs['tasks'][task]
            if not site_list:
                site_list = self.site_master_list
            tasks_df.loc[site_list, task] = True
        return tasks_df
    #--------------------------------------------------------------------------
          
    #--------------------------------------------------------------------------
    def get_site_list_for_task(self, task: str) -> list:
        """
        Return the list of sites for which task is enabled.

        Args:
            task: name of task.

        Returns:
            the list.

        """

        return self.tasks_df[self.tasks_df[task]==True].index.tolist()
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_excluded_sites_for_task(self, task, by_missing=True) -> list:
        
        return [
            site for site in self.site_master_list 
            if not site in self.get_site_list_for_task(task=task)
            ]
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END TASK MANAGER ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

tasks_mngr = TaskManager()
LOGGER_CONFIGS = cg.get_configs(config_name='py_logger')
logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN TASK DEFINITION FUNCTIONS ###
###############################################################################

### DATA CONSTRUCTORS ###   

#------------------------------------------------------------------------------
def construct_homogenised_TOA5(site: str) -> None:
    """Construct a TOA5 file for visualisation."""

    datacon = import_module(module_strs['data_constructors'])
    try:
        datacon.append_to_std_file(site=site)
    except FileNotFoundError:
        datacon.write_to_std_file(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_L1_nc(site: str) -> None:
    """Construct the L1 NetCDF file"""

    datacon = import_module(module_strs['data_constructors'])    
    try:
        datacon.append_to_current_nc_file(site)
    except FileNotFoundError:
        datacon.write_nc_year_file(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_site_details(site: str) -> None:
    """Construct the details file for the RTMC plotting"""
    
    deetcon = import_module(module_strs['details_constructor'])
    deetcon.write_site_info(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_status_xlsx() -> None:
    """Construct the status xlsx seeded with site list"""
    
    ns = import_module(module_strs['network_status'])
    ns.write_status_xlsx(
        site_list=tasks_mngr.get_site_list_for_task('construct_status_xlsx')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_status_geojson() -> None:
    """Construct the status geojson seeded with site list"""

    ns = import_module(module_strs['network_status'])    
    ns.write_status_geojson(
        site_list=tasks_mngr.get_site_list_for_task('construct_status_geojson')
        )
#------------------------------------------------------------------------------

### DATA PROCESSING ###

#------------------------------------------------------------------------------
def process_profile_data(site: str) -> None:
    
    pdp = import_module(module_strs['profile_processing'])
    output_path = pm.get_local_stream_path(
        resource='processed_data', 
        stream='profile', 
        site=site,
        )
    processor = pdp.load_site_profile_processor(site=site)
    processor.write_to_csv(file_name=output_path / 'storage_data.csv')
    processor.plot_diel_storage_mean(
        output_to_file=output_path / 'diel_storage_mean.png', open_window=False
        )
    processor.plot_diel_storage_mean(
        output_to_file=output_path / 'diel_storage_mean.png', open_window=False
        )
    processor.plot_vertical_evolution_mean(
        output_to_file=output_path / 'vertical_evolution_mean.png', 
        open_window=False
        )
    processor.plot_diel_storage_mean(
        output_to_file=output_path / 'diel_storage_mean.png', open_window=False
        )
#------------------------------------------------------------------------------

### LOCAL DATA MOVING ###

#------------------------------------------------------------------------------
def file_main_fast_data(site: str) -> None:
    
    ffd = import_module(module_strs['file_fast_data'])
    ffd.move_main_data(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def file_aux_fast_data(site: str) -> None:
    
    ffd = import_module(module_strs['file_fast_data'])
    ffd.move_aux_data(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _file_fast_data(site: str, which: str) -> None:
        
    ffd = import_module(module_strs['file_fast_data'])
    if which == 'main':
        ffd.move_main_data(site=site)
    if which == 'aux':
        ffd.move_aux_data(site=site)
    else:
        raise KeyError('`which` must be one of main or aux!')
#------------------------------------------------------------------------------

### RCLONE TRANSFERS - PULL TASKS

#------------------------------------------------------------------------------
def get_rclone_transfer_func(func: str):
    
    rt = import_module('file_transfers.rclone_transfer')
    funcs_dict = {
        
        # Pull tasks
        'pull_slow_rdm': rt.pull_slow_flux,
        'pull_RTMC_images': rt.pull_RTMC_images,
        'pull_profile_raw_rdm': rt.pull_profile_raw,
        'push_profile_raw_rdm': rt.push_profile_raw,
        
        # Push tasks
        'push_slow_rdm': rt.push_slow_flux,
        'push_main_fast_rdm': rt.push_main_fast_flux,
        'push_aux_fast_rdm': rt.push_aux_fast_flux,
        'push_RTMC_images': rt.push_RTMC_images,
        'push_homogenised_TOA5': rt.push_homogenised_TOA5,
        'push_L1_nc': rt.push_L1_nc,
        'push_L1_xlsx': rt.push_L1_xlsx,
        'push_status_geojson': rt.push_status_geojson,
        'push_status_xlsx': rt.push_status_xlsx,
        
        }
    
    return funcs_dict[func]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def pull_profile_raw(site: str) -> None:
    
    logger.info('Downloading data from remote location...')
    rct = import_module(module_strs['rclone_transfers'])
    local_location = pm.get_local_stream_path(
        resource='raw_data', stream='profile', site=site, as_str=True
        )
    remote_location = pm.get_remote_stream_path(
        resource='raw_data', stream='profile', site=site, as_str=True
        )
    rct.generic_move(
        local_location=local_location, 
        remote_location=remote_location, 
        which_way='from_remote'
        )
    logger.info('Done!')
#------------------------------------------------------------------------------

### S/FTP - PUSH TASKS ###

#------------------------------------------------------------------------------
def push_cosmoz(site: str) -> None:
    
    sftpt = import_module(module_strs['sftp_transfers'])
    sftpt.send_cosmoz(site=site)
#------------------------------------------------------------------------------

###############################################################################
### END TASK DEFINITION FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN DEFINE TASK FUNCTION DICTIONARY ###
###############################################################################

task_funcs = {

    # Data constructors
    'construct_homogenised_TOA5': construct_homogenised_TOA5,
    'construct_L1_xlsx': xlcon.construct_L1_xlsx,
    'construct_L1_nc': construct_L1_nc,
    'update_EddyPro_master': epc.update_eddypro_master,

    # Data processing
    'process_profile_data': process_profile_data,

    # Local data moving
    'file_main_fast_data': file_main_fast_data,
    'file_aux_fast_data': file_aux_fast_data,

    # Network status constructors
    'construct_status_xlsx': construct_status_xlsx,
    'construct_status_geojson': construct_status_geojson,    
    
    # Metadata constructors
    'construct_site_details_file': construct_site_details,
   
    # Rclone transfers - pull tasks
    'pull_slow_rdm': get_rclone_transfer_func(func='pull_slow_rdm'), #rt.pull_slow_flux,
    'pull_RTMC_images': get_rclone_transfer_func(func='pull_RTMC_images'),
    'pull_profile_raw_rdm': pull_profile_raw,
    
    # Rclone transfers - push tasks
    'push_slow_rdm': rt.push_slow_flux,
    'push_main_fast_rdm': rt.push_main_fast_flux,
    'push_aux_fast_rdm': rt.push_aux_fast_flux,
    'push_RTMC_images': rt.push_RTMC_images,
    'push_homogenised_TOA5': rt.push_homogenised_TOA5,
    'push_L1_nc': rt.push_L1_nc,
    'push_L1_xlsx': rt.push_L1_xlsx,
    'push_status_geojson': rt.push_status_geojson,
    'push_status_xlsx': rt.push_status_xlsx,
    
    # s/ftp - push tasks
    'push_cosmoz': push_cosmoz
    
    }

module_strs = {
    
    'profile_processing': 'profile_processing.profile_data_processor',
    'data_constructors': 'data_constructors.data_constructors',
    'details_constructors': 'data_constructors.details_constructor',
    'network_status': 'network_monitoring.network_status',
    'file_fast_data': 'file_handling.fast_data_filer',
    'rclone_transfers': 'file_transfers.rclone_transfer',
    'sftp_transfers': 'file_transfers.sftp_transfer'
    
    }

###############################################################################
### END DEFINE TASK FUNCTION DICTIONARY ###
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
def run_site_task(task: str, site:str) -> None:
    """
    Run a task for a single site (and log to single site log file).

    Args:
        task: name of task.
        site: name of site.

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
        function = task_funcs[task]
        function(site=site)
        logger.info('Task completed without error\n')
    except Exception:
        logger.error('Task failed with the following error:', exc_info=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_site_task_from_list(task: str) -> None:
    """
    Run a site task for a site (and log to single site log file) from list of sites.

    Args:
        task: name of task.

    Returns:
        None.

    """

    sites = tasks_mngr.get_site_list_for_task(task=task)
    for site in sites:
        run_site_task(task=task, site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def run_network_task(task: str) -> None:
    """
    Run a network-based task.

    Args:
        task: name of task.

    Returns:
        None.

    """

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
    function = task_funcs[task]

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

    if task not in task_funcs.keys():
        raise NotImplementedError(
            f'Function for task "{task}" not implemented!'
            )    

    task_func = task_funcs[task]
    args = list(inspect.signature(task_func).parameters.keys())
    if not args:
        run_network_task(task=task)
    elif args[0] == 'site':
        run_site_task_from_list(task=task)
    else:
        raise RuntimeError(f'Unknown task {task} passed!')
#------------------------------------------------------------------------------

###############################################################################
### END TASK MANAGEMENT FUNCTIONS ###
###############################################################################
 