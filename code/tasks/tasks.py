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

import datetime as dt
import inspect
import logging.config
import pandas as pd
import sys
from importlib import import_module

#------------------------------------------------------------------------------

from file_transfers import rclone_transfer as rct
from file_transfers import sftp_transfer as sftpt
from managers import paths

###############################################################################
### END IMPORTS ###
###############################################################################


###############################################################################
### BEGIN INITS ###
###############################################################################

LOGGER_CONFIGS = paths.get_internal_configs('py_logger')
logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN TASK MANAGER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class TaskManager():
    """
    Class to allow retrieval of task functions and sites for which tasks are
    enabled.

    """

    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        """
        Initialise manager with tasks x sites matrix and function dicts / lists.

        """

        self.configs = paths.get_internal_configs('tasks')
        self.site_master_list = self.configs['sites']
        self.master_tasks = list(self.configs['tasks'].keys())
        self._make_task_dataframe()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_task_dataframe(self) -> pd.DataFrame:
        """
        Make a dataframe with tasks x sites matrix.

        """

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
        self.tasks_df = tasks_df
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
    def get_excluded_sites_for_task(self, task) -> list:

        """
        Return the list of sites for which task is not enabled.

        Args:
            task: name of task.

        Returns:
            the list.

        """

        return [
            site for site in self.site_master_list
            if not site in self.get_site_list_for_task(task=task)
            ]
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

tasks_mngr = TaskManager()

###############################################################################
### END TASK MANAGER CLASS ###
###############################################################################



###############################################################################
### BEGIN TASK DEFINITION FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
### BEGIN DATA CONSTRUCTORS ###
#------------------------------------------------------------------------------

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

    # nctoa5 = import_module('data_constructors.nc_toa5_constructor_old')
    # datacon = nctoa5.NCtoTOA5Constructor(site=site)
    # datacon.write_to_TOA5()
    nctoa5 = import_module('data_constructors.nc_toa5_constructor')
    nctoa5.construct_visualisation_TOA5(site=site, n_files=3)
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

    pdp = import_module('profile_processing.profile_data_processor')
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

    fdf = import_module('file_handling.fast_data_filer')
    fdf.move_fast_files(site=site, is_aux=is_aux)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### END LOCAL DATA MOVING ###
#------------------------------------------------------------------------------

###############################################################################
### END TASK DEFINITION FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN FUNCTION FINDER CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class FunctionFinder():
    """
    Puts all public functions (both local and in called modules) in dictionary
    and sorts them into site-based and network-based functions.
    """

    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        """
        Do all the things.

        Returns:
            None.

        """

        task_functions = (
            dict(inspect.getmembers(sys.modules[__name__], inspect.isfunction)) |
            dict(inspect.getmembers(rct, inspect.isfunction)) |
            dict(inspect.getmembers(sftpt, inspect.isfunction))
            )
        task_functions = {
            name: func for name, func in task_functions.items()
            if not name.startswith('_')
            }
        network_tasks, site_tasks = [], []
        for name, func in task_functions.items():
            if name.startswith('_'):
                task_functions.pop(name)
            args = list(inspect.signature(func).parameters.keys())
            if not args:
                network_tasks.append(name)
            elif args[0] == 'site':
                site_tasks.append(name)
        self.tasks = list(task_functions.keys())
        self.task_functions = task_functions
        self.network_tasks = network_tasks
        self.site_tasks = site_tasks
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

# Instantiate finder at top level, since for site-based tasks, the finder must
# be repeatedly called.
func_finder = FunctionFinder()

###############################################################################
### END FUNCTION FINDER CLASS ###
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
        function = func_finder.task_functions[task]
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
    function = func_finder.task_functions[task]

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

    if task in func_finder.site_tasks:
        run_site_task_from_list(task=task)
    elif task in func_finder.network_tasks:
        run_network_task(task=task)
    else:
        raise NotImplementedError(
            f'Function for task "{task}" not implemented!'
            )
#------------------------------------------------------------------------------

###############################################################################
### END TASK MANAGEMENT FUNCTIONS ###
###############################################################################
