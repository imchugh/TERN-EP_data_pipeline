# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 09:43:07 2024

@author: jcutern-imchugh

Note that imports are embedded in the task function calls so that all modules
do not need to be loaded every time the task manager is called externally!

To do:
    fix profile processing and push / pull of raw and processed profile data (edit cron)


"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
import datetime as dt
import inspect
import logging.config
import pandas as pd
import sys
from importlib import import_module
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
# from data_constructors import data_constructors as datacon
# from data_constructors import details_constructor as deetcon
# from data_constructors import L1_workbook_constructor as xlcon
# from file_handling import eddypro_concatenator as epc
from file_transfers import rclone_transfer as rt

# from file_transfers import sftp_transfer as sftpt
# from network_monitoring import network_status as ns
# from utils import configs_getters as cg
from managers import paths
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

        self.configs = paths.get_internal_configs('tasks')
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
LOGGER_CONFIGS = paths.get_internal_configs('py_logger')
logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN TASK DEFINITION FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
### BEGIN DATA CONSTRUCTORS ###
#------------------------------------------------------------------------------

# module_strs = {

#     'profile_processing': 'profile_processing.profile_data_processor',
#     'data_constructors': 'data_constructors.data_constructors',
#     'details_constructors': 'data_constructors.details_constructor',
#     'network_status': 'network_monitoring.network_status',
#     'nc_constructors': 'data_constructors.nc_constructors',
#     'nc_toa5_constructors': 'data_constructors.nc_toa5_constructor',
#     'file_fast_data': 'file_handling.fast_data_filer',
#     'rclone_transfers': 'file_transfers.rclone_transfer',
#     'sftp_transfers': 'file_transfers.sftp_transfer'

#     }


#------------------------------------------------------------------------------
def construct_homogenised_TOA5(site: str) -> None:
    """Construct a TOA5 file for visualisation."""

    datacon = import_module('data_constructors.data_constructors')
    try:
        datacon.append_to_std_file(site=site)
    except FileNotFoundError:
        datacon.write_to_std_file(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_homogenised_TOA5_from_nc(site: str) -> None:

    nctoa5 = import_module('data_constructors.nc_toa5_constructor')
    datacon = nctoa5.NCtoTOA5Constructor(site=site)
    datacon.write_to_TOA5()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_L1_nc(site: str) -> None:
    """Construct the L1 NetCDF file"""

    nccon = import_module('data_constructors.nc_constructors')
    L1con = nccon.L1DataConstructor(site=site, constrain_start_to_flux=True)
    this_year = dt.datetime.now().year
    if max(L1con.data_years) == this_year:
        L1con.write_nc_file_by_year(year=this_year, overwrite=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_L1_xlsx(site: str) -> None:

    xlcon = import_module('data_constructors.L1_workbook_constructor')
    xlcon.construct_L1_xlsx(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def update_EddyPro_master(site: str) -> None:

    epc = import_module('file_handling.eddypro_concatenator')
    epc.update_eddypro_master(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_site_details(site: str) -> None:
    """Construct the details file for the RTMC plotting"""

    deetcon = import_module('data_constructors.details_constructor')
    deetcon.write_site_info(site=site)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_status_xlsx() -> None:
    """Construct the status xlsx seeded with site list"""

    ns = import_module('network_monitoring.network_status')
    ns.write_status_xlsx(
        site_list=tasks_mngr.get_site_list_for_task('construct_status_xlsx')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def construct_status_geojson() -> None:
    """Construct the status geojson seeded with site list"""

    ns = import_module('network_monitoring.network_status')
    ns.write_status_geojson(
        site_list=tasks_mngr.get_site_list_for_task('construct_status_geojson')
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### END DATA CONSTRUCTORS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### BEGIN DATA PROCESSING ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def process_profile_data(site: str) -> None:

    pdp = import_module(module_strs['profile_processing'])
    output_path = paths.get_local_stream_path(
        resource='processed_data',
        stream='profile',
        site=site,
        )
    processor = pdp.load_site_profile_processor(site=site)
    processor.write_to_csv(file_name=output_path / 'storage_data.csv')
    processor.plot_diel_storage_mean(
        output_to_file=output_path / 'diel_storage_mean.png', open_window=False
        )
    processor.plot_time_series(
        output_to_file=output_path / 'diel_storage_mean.png', open_window=False
        )
    processor.plot_vertical_evolution_mean(
        output_to_file=output_path / 'vertical_evolution_mean.png',
        open_window=False
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### END DATA PROCESSING ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### BEGIN LOCAL DATA MOVING ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def file_main_fast_data(site: str) -> None:

    _file_fast_data(site=site, is_aux=False)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def file_aux_fast_data(site: str) -> None:

    _file_fast_data(site=site, is_aux=True)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _file_fast_data(site: str, is_aux: bool) -> None:

    fdf = import_module(module_strs['file_fast_data'])
    fdf.move_fast_files(site=site, is_aux=is_aux)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### END LOCAL DATA MOVING ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### BEGIN RCLONE TRANSFERS - PULL TASKS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_rclone_transfer_func(func: str):

    rt = import_module('file_transfers.rclone_transfer')
    funcs_dict = {

        # Pull tasks
        'pull_slow_flux': rt.pull_slow_flux,
        'pull_RTMC_images': rt.pull_RTMC_images,
        'pull_profile_raw': rt.pull_profile_raw,
        'push_profile_raw': rt.push_profile_raw,

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
    local_location = paths.get_local_stream_path(
        resource='raw_data', stream='profile', site=site, as_str=True
        )
    remote_location = paths.get_remote_stream_path(
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
    'construct_homogenised_TOA5_from_nc': construct_homogenised_TOA5_from_nc,
    'construct_L1_xlsx': construct_L1_xlsx,
    'construct_L1_nc': construct_L1_nc,
    'update_EddyPro_master': update_EddyPro_master,

    # Data processing
    'process_profile_data': process_profile_data,

    # Local data moving
    'file_main_fast_data': file_main_fast_data,
    'file_aux_fast_data': file_aux_fast_data,

    # Network status constructors
    'construct_status_xlsx': construct_status_xlsx,
    'construct_status_geojson': construct_status_geojson,

    # Metadata constructors
    'construct_site_details': construct_site_details,

    # Rclone transfers - pull tasks
    'pull_slow_flux': rt.pull_slow_flux,
    'pull_RTMC_images': rt.pull_RTMC_images,
    'pull_profile_raw': pull_profile_raw,

    # Rclone transfers - push tasks
    'push_slow_flux': rt.push_slow_flux,
    'push_main_fast_flux': rt.push_main_fast_flux,
    'push_aux_fast_flux': rt.push_aux_fast_flux,
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
    'nc_constructors': 'data_constructors.nc_constructors',
    'nc_toa5_constructors': 'data_constructors.nc_toa5_constructor',
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
def get_funcs_dict():

    current_module = sys.modules[__name__]
    rt_module = import_module('file_transfers.rclone_transfer')
    rslt = (
        dict(inspect.getmembers(current_module, inspect.isfunction)) |
        dict(inspect.getmembers(rt_module, inspect.isfunction))
        )
    return {
        name: func for name, func in rslt.items() if not name.startswith('_')
        }
#------------------------------------------------------------------------------

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
        paths.get_local_stream_path(
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
        paths.get_local_stream_path(
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
