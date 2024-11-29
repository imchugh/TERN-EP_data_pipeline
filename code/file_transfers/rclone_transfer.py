# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 11:25:28 2022

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
# STANDARD IMPORTS
import logging
import pathlib
import subprocess as spc
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS
from paths import paths_manager as pm
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# INITS
APP_PATH = 'rclone'
ARGS_LIST = [
    'copy', '--transfers', '10', '--progress', '--checksum', '--timeout', '0'
    ]
logger = logging.getLogger(__name__)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generic_move(
        local_location, remote_location, which_way='to_remote',
        exclude_dirs=None, mod_time=True, timeout=600
        ):

    # Check direction is valid
    if not which_way in ['to_remote', 'from_remote']:
        raise KeyError('Arg "which_way" must be "to_remote" or "from_remote"')

    
    logger.info('Checking local and remote directories...')
    
    # Check local is valid
    if not pathlib.Path(local_location).exists:
        msg = f'    -> local file {str(local_location)} is not valid!'
        logger.error(msg); raise FileNotFoundError(msg)
    logger.info(f'    -> local directory {local_location} is valid')
    
    # Check remote is valid
    try:
        check_remote_available(str(remote_location))
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logger.error(e)
        logger.error(
            f'    -> remote location {remote_location} is not valid!'
            )
        raise
    logger.info(f'    -> remote location {remote_location} is valid')

    # Set from and to locations, based on direction
    if which_way == 'to_remote':
        from_location = local_location
        to_location = remote_location
    else:
        from_location = remote_location
        to_location = local_location

    # Add any arguments to the Rclone execution string
    run_args = ARGS_LIST.copy()
    if exclude_dirs:
        run_args += _add_rclone_exclude(exclude_dirs=exclude_dirs)
        
    # This is required to copy to the DSA web-sites directory and subs
    if not mod_time:
        run_args.append('--sftp-set-modtime=false')
        
    # Do the transfer
    logger.info('Copying now...')
    run_list =  [APP_PATH] + run_args + [from_location, to_location]
    try:
        rslt = _run_subprocess(run_list=run_list, timeout=timeout)
        logger.info(rslt.stdout.decode())
    except (spc.TimeoutExpired, spc.CalledProcessError) as e:
        logger.error(e)
        logger.error('Copy failed!')
        raise
    logger.info('Copy succeeded')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def pull_slow_flux(site):

    logger.info(f'Begin retrieval of {site} slow data from UQRDM')
    move_data_stream(
        site=site, stream='flux_slow', which_way='from_remote'
        )
    logger.info('Done')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_status_files():

    logger.info('Begin move of status files to UQRDM')
    generic_move(
        local_location=pm.get_local_resource_path(resource='network_status'),
        remote_location=pm.get_remote_data_path(data_stream='epcn_share'),
        timeout=180
        )
    logger.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_main_fast_flux(site):

    logger.info(f'Begin move of {site} fast data to UQRDM flux archive')
    move_data_stream(
        site=site, stream='flux_fast', exclude_dirs=['TMP'], timeout=1200
        )
    logger.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_aux_fast_flux(site):

    logger.info(f'Begin move of {site} fast data to UQRDM flux archive')
    move_data_stream(
        site=site, stream='flux_fast_aux', exclude_dirs=['TMP'], timeout=1200
        )
    logger.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_slow_flux(site):

    logger.info(f'Begin move of {site} slow flux data to UQRDM')
    move_data_stream(site=site, stream='flux_slow')
    logger.info('Done.')
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def push_profile_processed(site):

#     logger.info(f'Begin move of {site} processed profile data to UQRDM')
#     _move_site_data_stream(site=site, stream='profile_proc')
#     logger.info('Done.')
# #------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_status_geojson() -> None:
    """Use Rclone to push data to rdm"""

    _push_status_file(which='geojson')    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_status_xlsx() -> None:
    """Use Rclone to push data to rdm"""
    
    _push_status_file(which='xlsx')    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _push_status_file(which) -> None:
    """Helper function"""
    
    resource = 'network'
    stream = f'status_{which}'
    generic_move(
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

#------------------------------------------------------------------------------
def pull_RTMC_images():
    
    logger.info('Begin retrieval of RTMC images from Windows machine')
    generic_move(
        local_location=(
            pm.get_local_stream_path(
                resource='network', 
                stream='RTMC_images'
                )
            ), 
            remote_location=(
                pm.get_remote_stream_path(
                    resource='network', 
                    stream='RTMC_image_source'
                )
            ),
        which_way='from_remote',
        timeout=180
        )
    logger.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_RTMC_images():
    
    logger.info('Begin transfer of RTMC images to DSA')
    generic_move(
        local_location=(
            pm.get_local_stream_path(
                resource='network', 
                stream='RTMC_images'
                )
            ), 
            remote_location=(
                pm.get_remote_stream_path(
                    resource='network', 
                    stream='RTMC_image_dest'
                )
            ),
        which_way='to_remote',
        mod_time=False,
        timeout=600
        )
    logger.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_homogenised_TOA5():

    _push_homogenised(stream='TOA5')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_TOA5():

    _push_homogenised(stream='TOA5')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_L1_xlsx():
    
    _push_homogenised(stream='xlsx')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def push_L1_nc():
    
    _push_homogenised(stream='nc')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _push_homogenised(stream):
    
    allowed_streams = pm.list_local_streams(resource='homogenised_data')
    if not stream in allowed_streams:
        raise KeyError(f'`stream` must be one of {allowed_streams}')
    logger.info(f'Begin move of homogenised data {stream}')
    resource = 'homogenised_data'
    stream = stream
    generic_move(
        local_location=(
            pm.get_local_stream_path(resource=resource, stream=stream)
            ),
        remote_location=(
            pm.get_remote_stream_path(resource=resource, stream=stream)
            ),
        timeout=180
        )
    logger.info('Done.')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def move_data_stream(
        site, stream, exclude_dirs=None, which_way='to_remote', timeout=600
        ):

    local_path = _reformat_path_str(
        pm.get_local_stream_path(
            resource='raw_data', stream=stream, site=site, as_str=True
            )
        )
    remote_path = _reformat_path_str(
        pm.get_remote_stream_path(
            resource='raw_data', stream=stream, site=site, as_str=True
            )
        )
    generic_move(
        local_location=local_path, remote_location=remote_path,
        exclude_dirs=exclude_dirs, which_way=which_way, timeout=timeout
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_remote_available(remote_path):

    _run_subprocess(
        run_list=[APP_PATH, 'lsd', str(remote_path)],
        timeout=30
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _add_rclone_exclude(exclude_dirs):

    this_list = []
    for this_dir in exclude_dirs:
        this_list.append('--exclude')
        this_list.append(f'{this_dir}/**')
    return this_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _reformat_path_str(path_str):

    return path_str.replace('\\', '/')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _run_subprocess(run_list, timeout=5):

    return spc.run(
        run_list, capture_output=True, timeout=timeout, check=True
        )
#------------------------------------------------------------------------------