# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 15:46:11 2024

@author: jcutern-imchugh
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
import pathlib
import yaml
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### INITS ###
LOCAL_CONFIG_PATH_FILE = pathlib.Path(
    '/store/Config_files/Network/Paths/local_paths.yml'
    )
REMOTE_CONFIG_PATH_FILE = pathlib.Path(
    '/store/Config_files/Network/Paths/remote_paths.yml'
    )
REMOTE_ALIAS_DICT = {
    'AliceSpringsMulga': 'AliceMulga', 'Longreach': 'MitchellGrassRangeland'
    }
PLACEHOLDER = '<site>'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### GLOBALS ###
def _read_yml(file):
    
    with open(file) as f:
        return yaml.safe_load(stream=f)

local_paths = _read_yml(file=LOCAL_CONFIG_PATH_FILE)
remote_paths = _read_yml(file=REMOTE_CONFIG_PATH_FILE)
#------------------------------------------------------------------------------



###############################################################################
### BEGIN LOCAL DATA FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def list_local_resources() -> list:
    """List the local resources defined in the .yml file."""

    return list(local_paths.keys())
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def list_local_streams(resource: str) -> list:
    """
    List the local streams defined in the .yml file.

    Args:
        resource: resource for which to return streams.

    Returns:
        List of local streams.

    """

    return list(local_paths[resource]['stream'].keys())
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def list_applications() -> list:
    """
    List the available local applications defined in the .yml file."""

    return list(local_paths['applications']['stream'].keys())
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_local_resource_path(
        resource: str, **kwargs: dict
        ) -> pathlib.Path | str:
    """
    Get the path to the local resource.

    Args:
        resource: the local resource for which to return the path.
        **kwargs: additional keyword args to pass to get_path function.

    Returns:
        Path to local resource.

    """

    no_args = ['location', 'resource', 'stream']
    _kill_kwargs(kwargs=kwargs, kill_list=no_args)
    return get_path(location='local', resource=resource, **kwargs)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_local_stream_path(
        resource: str, stream: str, **kwargs: dict
        ) -> pathlib.Path | str:
    """
    Get the path to the local resource stream.

    Args:
        resource: the local resource for which to return the path.
        stream: resource stream for which to return the path.
        **kwargs: additional keyword args to pass to get_path method.

    Returns:
        Path to local resource stream.

    """

    no_args = ['location', 'resource', 'stream']
    _kill_kwargs(kwargs=kwargs, kill_list=no_args)
    return get_path(
        location='local', resource=resource, stream=stream, **kwargs
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_local_config_file(config_stream: str, **kwargs: dict) -> dict:
    
    return _read_yml(
        get_path(
            location='local', resource='configs', stream=config_stream, 
            **kwargs
            )
        )
#------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_site_image_path(img_type: str, **kwargs: dict) -> pathlib.Path | str:
#     """
#     Get the path to local site image.

#     Args:
#         img_type: Image type. Must be either 'tower' or 'contour'.
#         **kwargs: additional keyword args to pass to get_path method.

#     Returns:
#         Path to local site image.

#     """

#     no_args = ['stream', 'subdirs', 'file_name']
#     _kill_kwargs(kwargs=kwargs, kill_list=no_args)
#     img_dict = {'tower': '<site>_tower.jpg', 'contour': '<site>_contour.png'}
#     return get_path(
#             location='local', resource='network', stream='site_images',
#             file_name=img_dict[img_type], **kwargs
#             )
# #------------------------------------------------------------------------------

# #------------------------------------------------------------------------------
# def get_application_path(self, application: str) -> pathlib.Path | str:
#     """
#     Get the path to local application.

#     Args:
#         application: application name.

#     Returns:
#         Path to local application.

#     """

#     return self.get_path(
#         location='local', resource='applications', stream=application
#         )
# #------------------------------------------------------------------------------

###############################################################################
### END LOCAL DATA FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN REMOTE DATA FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def list_remote_resources() -> list:
    """
    List the available remote resources defined in the .yml file.

    Returns:
        List of remote resources.

    """

    return list(remote_paths.keys())
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def list_remote_streams(resource: str) -> pathlib.Path | str:
    """
    List the available remote streams defined in the .yml file.

    Args:
        resource: the remote resource for which to return the streams.

    Returns:
        List of remote resource streams.

    """

    return list(remote_paths[resource]['stream'].keys())
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_remote_resource_path(
        resource: str, **kwargs: dict
        ) -> pathlib.Path | str:
    """
    Get the path to the remote resource.

    Args:
        resource: the remote resource for which to return the path.
        **kwargs: additional keyword args to pass to get_path method.

    Returns:
        Path of remote resource.

    """

    no_args = ['location', 'resource', 'stream', 'subdirs', 'file_name']
    _kill_kwargs(kwargs=kwargs, kill_list=no_args)
    return get_path(
        location='remote', resource=resource, **kwargs
        )
#------------------------------------------------------------------------------

#--------------------------------------------------------------------------
def get_remote_stream_path(
        resource: str, stream: str, **kwargs: dict
        ) -> pathlib.Path | str:
    """
    Get the path to the remote resource stream.

    Args:
        resource: the remote resource for which to return the path.
        stream: resource stream for which to return the path.
        **kwargs: additional keyword args to pass to get_path method.

    Returns:
        Path to remote resource stream.

    """

    no_args = ['location', 'resource', 'stream']
    _kill_kwargs(kwargs=kwargs, kill_list=no_args)
    return get_path(
        location='remote', resource=resource, stream=stream, **kwargs
        )
#--------------------------------------------------------------------------

###############################################################################
### END REMOTE DATA FUNCTIONS ###
###############################################################################



###############################################################################
### BEGIN GENERIC DATA FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def get_path(
        location: str, resource: str, stream: str=None,
        site: str=None, subdirs: list=[], file_name: str=None,
        check_exists: bool=False, as_str: bool=False
        ):
    """
    Get the path to a resource or stream.

    Args:
        location: local or remote.
        resource: resource to select.
        stream (optional): stream path to extract. Defaults to None.
        site (optional): name of site. Defaults to None.
        subdirs (optional): list of subdirectories to append. Defaults to [].
        file_name (optional): file name to append. Defaults to None.
        check_exists (optional): whether to check if path exists. Defaults to False.
        as_str (optional): whether to return path as str. Defaults to False.

    Raises:
        KeyError: raised (and caught) if passed site doesn't have a remote alias.
        RuntimeError: raised if subdirectories or file names are passed but the path is to a file.
        FileNotFoundError: raised if check_exists=True and file does not exist.

    Returns:
        Path to resource / stream.

    """

    # Select local or remote configs
    if location == 'local':
        configs = local_paths
    elif location == 'remote':
        configs = remote_paths
    else:
        raise KeyError('Allowed locations are "local" and "remote"!')

    # Create an empty path
    path = pathlib.Path()

    # get the configs for the specific resource
    resource_configs = configs[resource]
    if len(resource_configs['base_path']) > 0:
        path = path / resource_configs['base_path']

    # Add the resource stream to the path
    if not stream is None:
        path = path / resource_configs['stream'][stream]

    # Check if resolves to a file; if so, subdirs and file_name cannot be
    # sensibly appended -> raise
    if len(path.suffix) > 0:
        if len(subdirs) > 0:
            raise RuntimeError(
                f'Current path resolves to a file ({path.name}); no '
                'subdirectory append allowed!'
                )
        if not file_name is None:
            raise RuntimeError(
                f'Current path resolves to a file ({path.name}); no '
                'file name append allowed!'
                )

    # Add subdirectories
    subdirs = list(subdirs)
    if not file_name is None:
        subdirs.append(file_name)
    for element in subdirs:
        path = path / element

    # Fill site placeholder (and get remote alias if applicable)
    if not site is None:
        if location == 'remote':
            try:
                site = REMOTE_ALIAS_DICT[site]
            except KeyError:
                pass
        path = pathlib.Path(str(path).replace(PLACEHOLDER, site))

    # Check if the path exists
    if check_exists:
        if not path.exists():
            raise FileNotFoundError('No such path!')

    # Return
    if as_str:
        return str(path)
    return path
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _kill_kwargs(kwargs, kill_list):

    [kwargs.pop(arg, None) for arg in kill_list]
#------------------------------------------------------------------------------

###############################################################################
### END GENERIC DATA FUNCTIONS ###
###############################################################################
