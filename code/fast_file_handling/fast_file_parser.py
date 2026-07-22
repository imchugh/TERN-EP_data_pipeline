#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 18 11:44:11 2026

@author: imchugh

Campbell Scientific File Tools
==============================

Domain model:

    CSFile            -> represents one file
    DailyPartitioner  -> partitions one file into a subset of files

Responsibilities are strictly separated.
"""

###############################################################################
# BEGIN IMPORTS
###############################################################################

import pathlib
import numpy as np
import pandas as pd

from file_handling import read_cs_files as rcf

###############################################################################
# END IMPORTS
###############################################################################


###############################################################################
### BEGIN INITS ###
###############################################################################

INFO_NAMES = [
    "format", "station_name", "logger_type",
    "serial_num", "OS_version", "program_name", "program_sig"
    ]

_FORMAT_INFO_KEY = {
    "TOB3": "creation_date",
    "TOB1": "table_name",
    }

###############################################################################
### END INITS ###
###############################################################################


###############################################################################
### BEGIN CLASSES ###
###############################################################################

# -----------------------------------------------------------------------------
class CSFileConverter:
    """
    Represents a single Campbell Scientific file.
    """

    # -------------------------------------------------------------------------
    
    def __init__(self, file: pathlib.Path | str):
        """
        Initialise the class with empty attrs.

        Args:
            file: absolute path to file.

        Raises:
            FileNotFoundError: raised if file does not exist.

        Returns:
            None.

        """
        
        self.file = pathlib.Path(file)
        if not self.file.exists():
            raise FileNotFoundError(f"{self.file} does not exist")
        self.input_format: str | None = None
        self.metadata: list | None = None
        self.ex_meta: list | None = None
        self.data: pd.DataFrame | None = None
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------

    def load(self) -> 'CSFileConverter':
        """
        Load the data.

        Returns:
            'CSFileConverter': the populated converter.

        """

        contents, metadata = rcf.read_cs_files(self.file)
        metadata = [
            [elem.strip() for elem in sublist] for sublist in metadata
            ]
        self.input_format = metadata[0][0]
        if self.input_format == "TOB3":
            self.ex_meta = metadata.pop(1)
        self.metadata = metadata

        data_headers = metadata[1]
        self.data = (
            pd.DataFrame(
                dict(zip(data_headers, contents))
            )
            .set_index("TIMESTAMP")
        )
        self.data = _optimise_numeric_dtypes(self.data)

        return self   
    # -------------------------------------------------------------------------

    # ------------------------------------------------------------------

    def get_file_info(self) -> dict:
        """
        Get the logger information from the first line of the metadata.

        Returns:
            info fields in dict.

        """

        last_key = _FORMAT_INFO_KEY[self.input_format]
        keys = INFO_NAMES.copy()
        keys.append(last_key)
        return dict(zip(keys, self.metadata[0]))
    # -------------------------------------------------------------------------
    
    # ------------------------------------------------------------------

    def get_file_header(self) -> pd.DataFrame:
        """
        Get all header lines after the info line.

        Returns:
            dataframe containing the lines.

        """

        return (
            pd.DataFrame(
                data=self.metadata[1:4],
                index=["variable", "units", "sampling"]
            )
            .T
            .set_index("variable")
        )
    # -------------------------------------------------------------------------
    
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
class DailyPartitioner:
    """
    Partition a single TOB3 CSFile into fixed-length time blocks.

    Should be created via the helper function `get_daily_partitioner()` or
    directly with a CSFile (loaded or not).
    """

    # -------------------------------------------------------------------------\
        
    def __init__(
        self, csfile: CSFileConverter, time_step: int, freq_hz: int
        ) -> 'DailyPartitioner':
        """
        Initialise empty object.

        Args:
            csfile: a CSFile object (loaded or not).
            time_step: site averaging interval in minutes.
            freq_hz: data frequency in Hz.

        Raises:
            TypeError: raised if the file is not TOB3.

        Returns:
            None.

        """

        # Load if needed
        if csfile.data is None:
            csfile.load()

        if csfile.input_format != "TOB3":
            raise TypeError("DailyPartitioner requires a TOB3 file")

        self.csfile = csfile
        self.data = csfile.data
        self.time_step = time_step
        self.freq_hz = freq_hz
        self.n_expected = time_step * freq_hz * 60

        self.file_reference = self._build_reference()
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------

    def _build_reference(self) -> pd.DataFrame:
        """
        Build reference table for partitioned blocks.

        Returns:
            a DataFrame with columns: start_date, end_date, n_recs.

        """

        file_start = self.data.index[0]
        day_start = file_start.normalize()
        day_end = day_start + pd.Timedelta(days=1)

        starts = pd.date_range(
            start=day_start,
            end=day_end,
            freq=f"{self.time_step}min",
            inclusive="left"
        )
        ends = starts + pd.Timedelta(minutes=self.time_step)

        # Count records without overlap using 'between' with left-inclusive, right-exclusive
        n_recs = [
            self.data.index.to_series().between(start, end, inclusive="left").sum()
            for start, end in zip(starts, ends)
        ]

        return pd.DataFrame(
            {"start_date": starts, "end_date": ends, "n_recs": n_recs},
            index=pd.RangeIndex(len(starts), name="file_num")
        )
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------

    def get_data_by_file_num(self, num: int) -> pd.DataFrame:
        """
        Return the subset of data corresponding to a block number.

        Args:
            num: block number.

        Raises:
            RuntimeError: raised if no data in block.

        Returns:
            the data in a dataframe.

        """

        rec = self.file_reference.loc[num]
        if rec.n_recs == 0:
            raise RuntimeError(
                f"No data between {rec.start_date:%H:%M} and {rec.end_date:%H:%M}"
            )

        # Use left-inclusive, right-exclusive slicing to match _build_reference
        mask = (
            self.data.index.to_series()
            .between(rec.start_date, rec.end_date, inclusive="left")
            )
        return self.data.loc[mask]
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------

    def get_file_reference(self) -> pd.DataFrame:
        """
        Return the reference table containing start/end timestamps and 
        record counts.

        Returns:
            reference table.
        """

        return self.file_reference.copy()
    # -------------------------------------------------------------------------


# -----------------------------------------------------------------------------

###############################################################################
### END CLASSES ###
###############################################################################


###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

# -----------------------------------------------------------------------------

def get_daily_partitioner(
        file: str | pathlib.Path, time_step: int, freq_hz: int
        ) -> DailyPartitioner:
    """
    Factory function to return a DailyPartitioner for a TOB3 file.    

    Args:
        file: absolute path to file.
        time_step: site averaging interval in minutes.
        freq_hz: data frequency in Hz.

    Returns:
        DailyPartitioner.

    """

    csfile = CSFileConverter(file=file)
    return DailyPartitioner(csfile, time_step, freq_hz)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def _optimise_numeric_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimise numeric columns for TOA5/TOB3 output.

    - Float columns that are integer-like → Int32
    - Other float columns → float32 rounded to 7 significant digits (using f-string method)
    

    Args:
        df: the data.

    Returns:
        the formatted data.

    """
    
    # Copy passed data
    df = df.copy()

    # Iterate over columns
    for col in df.select_dtypes(include="float"):
        if _check_integer_data(df[col]):
    
            # Replace ±inf with NA
            df[col] = df[col].replace([np.inf, -np.inf], pd.NA)
    
            # Replace values outside Int32 range with NA
            max_int32 = 2_147_483_647
            min_int32 = -2_147_483_648
            mask_out_of_bounds = (df[col] > max_int32) | (df[col] < min_int32)
            df[col] = df[col].where(~mask_out_of_bounds, pd.NA)
    
            # Convert integer-like floats to Int32
            try:
                df[col] = df[col].astype("Int32")
            except (TypeError, OverflowError):
                df[col] = pd.to_numeric(
                    df[col],
                    downcast="integer",
                    errors="coerce"
                )
        else:
            # Keep as float32 and round to 7 significant digits using f-string
            df[col] = df[col].astype("float32").apply(lambda x: float(f"{x:.7g}"))

    return df
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

def _check_integer_data(series: pd.Series) -> bool:
    """
    Return True if all values in the series are effectively integers.
    NaNs are ignored.    

    Args:
        series: expected numerical series.

    Returns:
        returns True if is integer, else false.

    """
    
    arr = series.to_numpy(dtype=float)
    return np.isclose(arr, np.round(arr), equal_nan=True).all()
# -----------------------------------------------------------------------------
