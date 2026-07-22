#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 14:34:02 2026

@author: imchugh
Simple validation function to ensure that (in order):
    1) the file names specified in the site variable map exist in the directory 
    structure, and; 
    2) the variable names are found in the header of the specified file.
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

from pathlib import Path
from infrastructure.file_io import read_lines

###############################################################################
### END IMPORTS ###
###############################################################################


###############################################################################
### BEGIN CLASSES ###
###############################################################################

def validate_raw_data_integrity(file_map: dict[Path, list[str]]) -> None:
    """
    Validate raw data files exist and contain expected variables.
    """

    for file_path, variables in file_map.items():

        if not file_path.exists():
            raise FileNotFoundError(f"Expected file missing: {file_path}")

        # header_line = read_lines(path=file_path, n=2)[1]
        header_line = read_lines(file=file_path, begin=1, end=1)[0]

        # header = {x.strip() for x in header_line.split(",")}
        header = set(header_line)

        missing = [v for v in variables if v not in header]

        if missing:
            raise ValueError(
                f"{file_path} missing variables: {missing}"
            )
                
###############################################################################
### END CLASSES ###
###############################################################################                