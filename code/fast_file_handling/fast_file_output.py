#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 19 10:53:27 2026

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import csv
import logging
import pathlib
import pandas as pd
import hashlib

###############################################################################
### END IMPORTS ###
###############################################################################


###############################################################################
### BEGIN INITS ###
###############################################################################

# Constants
CSV_SEP = ","
CSV_NA_VALUES = "NAN"

logger = logging.getLogger(__name__)

###############################################################################
### END INITS ###
###############################################################################


###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

# -----------------------------------------------------------------------------

def _format_output_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Format partitioned data block into TOA5-compatible structure.
    - Convert bool columns to int
    - Reset index
    - Format TIMESTAMP to 100ms precision
    """

    data = data.copy()

    # Convert bool → int
    bool_cols = data.select_dtypes(bool).columns
    if len(bool_cols):
        data[bool_cols] = data[bool_cols].astype(int)

    data = data.reset_index()

    # Round to nearest 100ms with microsecond bias
    ts = data["TIMESTAMP"] + pd.Timedelta(microseconds=500)
    ts = ts.dt.round("100ms")

    base = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    tenths = (ts.dt.microsecond // 100_000).astype(str)

    data["TIMESTAMP"] = [
        b if t == "0" else f"{b}.{t}"
        for b, t in zip(base, tenths)
    ]

    return data
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def write_toa5_file(
    output_file: pathlib.Path,
    headers: list,
    data: pd.DataFrame,
    ) -> None:
    """
    Write TOA5 block atomically.

    - Ensures parent directory exists
    - Writes to temporary file
    - Atomically replaces destination
    """

    # Ensure passed file is a Path object, and create parent directory
    output_file = pathlib.Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Create temp file
    tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")

    # Format data for output
    formatted_data = _format_output_data(data=data)

    # Write file
    with open(tmp_file, "w", newline="\n") as f:
        header_writer = csv.writer(f, delimiter=CSV_SEP, quoting=csv.QUOTE_ALL)
        for row in headers:
            header_writer.writerow(row)
        formatted_data.to_csv(
            f,
            header=False,
            index=False,
            na_rep=CSV_NA_VALUES,
            sep=CSV_SEP,
            quoting=csv.QUOTE_NONNUMERIC,
            )

    # Atomic replace (same filesystem)
    tmp_file.replace(output_file)
    
    logger.info(f"Wrote TOA5 file: {output_file.name}")
# -----------------------------------------------------------------------------        

# # -----------------------------------------------------------------------------

# def write_toa5_block(
#     output_file: pathlib.Path | str,
#     data_block: pd.DataFrame,
#     headers: list
#     # file_info: dict,
#     # file_header: pd.DataFrame,
#     ) -> None:
#     """
#     Write a single TOA5 partition block.

#     This is the only public entry point for writing.
#     """

#     output_file = pathlib.Path(output_file)
#     formatted_data = _format_output_data(data_block)
#     _write_data_to_file(
#         file=output_file, 
#         headers=headers, 
#         data=formatted_data
#         )
#     logger.info(f"Wrote TOA5 block: {output_file.name}")
# # -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def safe_move(src: pathlib.Path, dst: pathlib.Path, check_hash: bool = True):
    """
    Move a file safely. Raises FileExistsError if destination exists and is different.

    Args:
        src: source file path
        dst: destination file path
        check_hash: whether to verify contents before moving

    Raises:
        FileExistsError: if dst exists and is different from src
        FileNotFoundError: if src does not exist
    """
    src = pathlib.Path(src)
    dst = pathlib.Path(dst)

    if not src.exists():
        raise FileNotFoundError(f"Source file {src} does not exist")

    # Ensure parent directory exists
    dst.parent.mkdir(parents=True, exist_ok=True)

    if check_hash and dst.exists():
        if get_file_hash(src) != get_file_hash(dst):
            raise FileExistsError(f"Non-identical file exists at {dst}")
        else:
            logger.info(f"Destination {dst} already exists and is identical, skipping move")
            return

    # Atomic move if on same filesystem
    src.replace(dst)
    logger.info(f"Moved {src} -> {dst}")
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def get_file_hash(file: pathlib.Path | str) -> str:
    """
    

    Args:
        file (pathlib.Path | str): DESCRIPTION.

    Returns:
        str: DESCRIPTION.

    """
    
    file = pathlib.Path(file)
    h = hashlib.sha256()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
# -----------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
