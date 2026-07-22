#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 13:35:13 2026

@author: imchugh
"""

import csv
import json
import logging
import pandas as pd
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------

def get_most_recent_file(
    *,
    root: Path,
    pattern: str = "*",
    recursive: bool = False,
    ) -> Path | None:
    """
    Return most recent file in directory matching pattern.

    Infrastructure-level utility.
    Returns None if no matching files.
    """

    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    iterator = root.rglob(pattern) if recursive else root.glob(pattern)

    files = [p for p in iterator if p.is_file()]

    if not files:
        return None

    return max(files, key=lambda p: p.stat().st_mtime)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def write_json_file(
    *,
    path: Path,
    data: list[dict],
    run_id: str,
) -> None:

    logger.info(
        "json_write_start",
        extra={
            "run_id": run_id,
            "path": str(path),
        },
    )

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    logger.info(
        "json_write_complete",
        extra={
            "run_id": run_id,
            "path": str(path),
            "records": len(data),
        },
    )
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------    

def read_yml(path: Path) -> dict:
    
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def read_json(path: Path):
    
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def read_text(path: Path, encoding="utf-8") -> str:
    with open(path, "r", encoding=encoding) as f:
        return f.read()
# -----------------------------------------------------------------------------    

#------------------------------------------------------------------------------
def read_lines(
        file: str | Path, begin: int=0, end: int=4, sep: str=','
        ) -> list:
    """Get a list of the header strings.

    Args:
        file: absolute path of file to parse.
        begin: line number of first header line.
        end: line number of last header line.
        sep: text separation character.

    Returns:
        List of sublists, each sublist containing the text elements of a header
            line.

    """

    line_list = []
    with open(file, 'r') as f:
        for i in range(end + 1):
            line = f.readline()
            if not i < begin:
                line_list.append(line)
    return [line for line in csv.reader(line_list, delimiter=sep)]
#------------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def read_csv_data(path: str, file_format: dict, **kwargs) -> pd.DataFrame:
    """
    Reads a CSV/TSV file according to the provided file_format dictionary.

    Parameters
    ----------
    path : str
        Path to the data file.
    file_format : dict
        Dictionary containing parsing information. Expected keys:
        - info_line: number of initial info lines to skip
        - header_lines: dict with keys 'variable', 'units', 'sampling'
        - separator: str, column separator
        - non_numeric_cols: list of columns to treat as strings
        - time_variables: dict mapping time columns to indices
        - na_values: value(s) to treat as NaN
        - quoting: quoting level (0,1,2)
    **kwargs
        Any additional keyword arguments are passed directly to pd.read_csv.

    Returns
    -------
    pd.DataFrame
    """
    
    # Extract skiprows and header line
    info_line = file_format.get("info_line") or 0
    header_lines = file_format.get("header_lines", {})
    header_line = header_lines.get("variable", 0)
    
    # Skip all non-variable lines: info + any other headers except variable
    other_headers = set(header_lines.values()) - {header_line}
    skiprows = sorted(set(range(info_line)) | other_headers)

    # Determine header row (variable names)
    header_line = file_format.get("header_lines", {}).get("variable", 0)

    # Handle quoting
    quoting = file_format.get("quoting", 0)

    # Columns to treat as strings
    dtype = {col: str for col in file_format.get("non_numeric_cols", [])}

    # Separator
    sep = file_format.get("separator", ",")

    # NA values
    na_values = file_format.get("na_values", None)

    # Read the file
    return pd.read_csv(
        path,
        sep=sep,
        skiprows=skiprows,
        header=header_line,
        dtype=dtype,
        na_values=na_values,
        quoting=quoting,
        **kwargs  # pass any extra kwargs
        )
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)
# -----------------------------------------------------------------------------