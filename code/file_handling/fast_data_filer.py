#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 11:03:58 2024

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
import datetime as dt
import logging
import pandas as pd
import pathlib
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CUSTOM IMPORTS ###
from managers import paths
from managers import site_details
#------------------------------------------------------------------------------

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################



###############################################################################
### BEGIN FAST DATA FILE INFO CLASS ###
###############################################################################

#------------------------------------------------------------------------------
class FastDataFileInfo():
    """
    Helper class to generate info about files available on the server for
    processing.
    """

    #--------------------------------------------------------------------------
    def __init__(self, site: str, is_aux: bool=False) -> None:
        """
        Gather critical information and set attributes, including underlying
        dataframe.

        Args:
            site: name of site.
            is_aux (optional): if True, processes the auxiliary EC system. |
            Defaults to False.

        Returns:
            None.

        """

        # Get file path
        self.site = site
        self.interval = get_site_fast_data_interval(site=site)
        self.input_dir = get_site_input_directory(site=site, is_aux=is_aux)
        self.output_base_dir = self.input_dir.parent
        self.reference_df = self._make_file_df()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_file_df(self) -> pd.DataFrame:
        """
        Initialisation method to build an underlying dataframe that other
        methods reference.

        Returns:
            df: dataframe containing file input / output location info.

        """

        # Create preliminary dataframe
        in_paths = sorted(list(self.input_dir.iterdir()))
        dates = [
            get_TOB3_file_creation_date(file=file).date()
            for file in in_paths
            ]
        out_dirs = [date.strftime('%Y_%m') for date in dates]
        df = (
            pd.DataFrame(
                data=zip(
                    in_paths, dates, out_dirs
                    ),
                index=pd.Index(
                    data=[in_file.name for in_file in in_paths],
                    name='infile_name'
                    ),
                columns=['infile_path', 'header_date', 'outfile_dir']
                )
            )

        # Amend output names where multiple files on same day
        df['outfile_name'] = None
        for date, sub_df in df.groupby('header_date'):
            str_date = date.strftime('%Y_%m_%d')
            if len(sub_df) == 1:
                num_appender = ['']
            else:
                num_appender = [
                    '_' + str(i).zfill(2) for i in range(len(sub_df))
                    ]
            for i, target in enumerate(sub_df.index):
                df.loc[target, 'outfile_name'] = (
                    f'TOB3_{self.site}_{self.interval}_'
                    f'{str_date}{num_appender[i]}.dat'
                    )

        # Create the complete output path
        df['outfile_path'] = (
            self.output_base_dir / df.outfile_dir / df.outfile_name
            )

        # Create a boolean indicating whether file is from current day
        df['today'] = df.header_date == dt.datetime.today().date()

        # Done
        return df
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_input_files(
            self, abs_path: bool=False, exclude_today: bool=True
            ) -> list:
        """
        List the input files available for processing.

        Args:
            abs_path (optional): If true, attach full path to file name. |
            Defaults to False.
            exclude_today (optional): If true, exclude any files that arrived |
            today. Defaults to True.

        Returns:
            list of input files.

        """

        df = self.reference_df.copy().reset_index()
        if exclude_today:
            df = df[~df.today]
        if not abs_path:
            return df.infile_name.tolist()
        return df.infile_path.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_input_file_info(self, file_name: str) -> pd.Series:
        """
        Get the information for a particular input file.

        Args:
            file_name: obvs.

        Returns:
            file-specific fields from the dataframe.

        """

        return self.reference_df.loc[file_name]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_output_files(
            self, abs_path: bool=False, exclude_today: bool=True
            ) -> list:
        """
        List the output file names.

        Args:
            abs_path (optional): If true, attach full path to file name. |
            Defaults to False.
            exclude_today (optional): If true, exclude any files that arrived |
            today. Defaults to True.

        Returns:
            list of output files.

        """

        df = self.reference_df.copy()
        if exclude_today:
            df = df[~df.today]
        if not abs_path:
            return df.outfile_name.tolist()
        return df.outfile_path.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_output_directories(self, exclude_today: bool=True) -> list:
        """
        Get a list of the unique subdirectories required for the output files.

        Args:
            exclude_today (optional): If true, exclude any files that arrived |
            today. Defaults to True.

        Returns:
            list of unique directories.

        """

        df = self.reference_df.copy()
        if exclude_today:
            df = df[~df.today]
        return df.outfile_dir.unique().tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def map_input_2_output(
            self, abs_path: bool=False, exclude_today: bool=True
            ) -> dict:
        """
        Create a dictionary mapping old file (key) to new file (value).

        Args:
            abs_path (optional): If true, attach full path to file name. |
            Defaults to False.
            exclude_today (optional): If true, exclude any files that arrived |
            today. Defaults to True.

        Returns:
            dictionary containing the mapping.

        """

        return dict(zip(
            self.get_input_files(
                abs_path=abs_path, exclude_today=exclude_today
                ),
            self.get_output_files(
                abs_path=abs_path, exclude_today=exclude_today
                )
            ))
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

###############################################################################
### END FAST DATA FILE INFO CLASS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

#------------------------------------------------------------------------------
def get_last_formatted_fast_file(
        site: str, is_aux: bool=False, abs_path: bool=True
        ) -> str:
    """
    Recursive glob to find the newest name-formatted file among subdirectories.

    Args:
        site: name of site.
        is_aux (optional): if True, check the storage area for the auxiliary
        EC system. Defaults to False.
        abs_path (optional): If true, attach full path to file name. |
        Defaults to False.

    Returns:
        name of file.

    """
    try:
        rslt = (
            sorted(
                [
                    x for x in get_site_output_directory(site=site, is_aux=is_aux)
                    .rglob(f'TOB3_{site}*.dat') if 'TMP' not in str(x.parent)
                    ]
                )
            [-1]
            )
    except IndexError:
        raise FileNotFoundError('No files')
    if abs_path:
        return rslt
    return rslt.name
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_input_directory(site: str, is_aux: bool=False) -> pathlib.Path:
    """
    Use the paths module to find the input directory for the site fast data.

    Args:
        site: name of site.
        is_aux (optional): if True, get the directory for the auxiliary
        EC system. Defaults to False.

    Returns:
        absolute path of directory.

    """

    stream = 'flux_fast'
    if is_aux:
        stream += '_aux'
    return paths.get_local_stream_path(
        resource='raw_data',
        stream=stream,
        site=site,
        subdirs=['TMP'],
        check_exists=True
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_output_directory(site: str, is_aux: bool=False) -> pathlib.Path:
    """
    Get the fast data output directory (parent of input directory).

    Args:
        site: name of site.
        is_aux (optional): if True, get the directory for the auxiliary
        EC system. Defaults to False.

    Returns:
        absolute path of directory.

    """

    return get_site_input_directory(site=site, is_aux=is_aux).parent
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_fast_data_interval(site: str) -> str:
    """
    Get the formatted site fast measurement interval.

    Args:
        site: name of str.

    Returns:
        string representation of frequency including millisecond units.

    """

    return (
        str(int(
            site_details
            .SiteDetailsManager(use_local=True)
            .get_single_site_details(
                site=site,
                field='freq_hz'
                )
            ))
        + '0ms'
        )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def move_fast_files(site: str, is_aux: bool=False) -> None:
    """
    Format and move to final storage any files available in the TMP directory.

    Args:
        site: name of site.
        is_aux (optional): if True, processes the auxiliary EC system. |
        Defaults to False.

    Returns:
        None.

    """

    logger.info('Beginning filing of card-based TOB3 fast data:')
    filer = FastDataFileInfo(site=site, is_aux=is_aux)
    files = filer.get_input_files()
    if len(files) == 0:
        logger.error('  No new files to process!')
        logger.info('Done')
        return
    logger.info('  Found new files! Processing...')
    for file in files:
        file_info = filer.get_input_file_info(file_name=file)
        target_dir = filer.output_base_dir / file_info.outfile_dir
        if not target_dir.exists():
            logger.info(f'    Creating year_month directory {target_dir}...')
            target_dir.mkdir()
        logger.info(
            f'    {file_info.name} -> {file_info.outfile_name}'
            )
        file_info.infile_path.rename(file_info.outfile_path)
    logger.info('Done!')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_TOB3_file_creation_date(file):
    """
    Read the header of the binary file to capture the file creation date.

    Args:
        file: absolute path to file.

    Returns:
        python datetime representation of header date.

    """

    with open(file=file, mode='rb') as f:
        return dt.datetime.strptime(
            (
                f.readline()
                .decode()
                .strip()
                .split(',')[-1]
                ),
            '"%Y-%m-%d %H:%M:%S"'
            )
#------------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
