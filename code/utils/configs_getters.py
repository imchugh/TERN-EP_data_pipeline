# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 15:45:33 2024

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
# STANDARD IMPORTS #
import pathlib
import yaml
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS #
# from file_handling.file_io import read_yml
from paths import paths_manager as pm
# import PathsManager
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# INITS #
config_paths = {
    stream: pm.get_local_stream_path(resource='configs', stream=stream)
    for stream in pm.list_local_streams(resource='configs')
    }
#------------------------------------------------------------------------------

###############################################################################
### BEGIN GLOBAL CONFIGURATION RETRIEVAL FUNCTIONS ###
###############################################################################

# #------------------------------------------------------------------------------
# def get_global_configs(which: str) -> dict:

#     allowed_which = [
#         key for key, value in config_paths.items() if 'Global' in str(value)
#         ]
#     if not which in allowed_which:
#         raise KeyError(
#             f'`which` kwarg must be one of {", ".join(allowed_which)}!'
#             )
#     return read_yml(config_paths[which])
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_task_configs() -> dict:

#     return read_yml(file=config_paths['tasks'])
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_all_sites_task_configs() -> dict:

#     return get_task_configs()['site_tasks']
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_network_task_configs() -> dict:

#     return get_task_configs()['network_tasks']
# #------------------------------------------------------------------------------

###############################################################################
### END GLOBAL CONFIGURATION RETRIEVAL FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN SITE-BASED CONFIGURATION RETRIEVAL FUNCTIONS ###
###############################################################################

# #------------------------------------------------------------------------------
# def get_site_hardware_configs(site: str, which: str=None) -> dict:

#     specific_path = _insert_site(file_path=config_paths['hardware'], site=site)
#     rslt = read_yml(file=specific_path)
#     if not which:
#         return rslt
#     return rslt[which]
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_site_variable_configs(site: str, which: str) -> dict:

#     specific_path = _insert_site(
#         file_path=config_paths[f'variables_{which}'], site=site
#         )
#     return read_yml(file=specific_path)
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_site_task_configs(site: str) -> dict:

#     return get_all_sites_task_configs()[site]
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_site_details_configs(site: str) -> dict:

#     specific_path = _insert_site(
#         file_path=config_paths['site_details'], site=site
#         )
#     return read_yml(file=specific_path)
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def _insert_site(file_path: pathlib.Path | str, site: str) -> pathlib.Path:

#     return pathlib.Path(str(file_path).replace(pm.PLACEHOLDER, site))
# #------------------------------------------------------------------------------

###############################################################################
### END SITE-BASED CONFIGURATION RETRIEVAL FUNCTIONS ###
###############################################################################

def list_available_config_names():
    
    return [
        file.stem for file in
        (pathlib.Path(__file__).resolve().parents[1] / 'configs').glob('*.yml')
        ]
    
def get_configs(config_name):
    
    path = (
        pathlib.Path(__file__).resolve().parents[1] / 
        'configs' / 
        f'{config_name}.yml'
        )
    with open(path) as f:
        return yaml.safe_load(stream=f)