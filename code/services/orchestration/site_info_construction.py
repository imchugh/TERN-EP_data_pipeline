#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 13:39:03 2026

@author: imchugh
"""

import datetime as dt
import logging
import time

from infrastructure.file_io import get_most_recent_file, write_json_file
from infrastructure import paths
from infrastructure.convert_calc_filter import TimeFunctions
from services.foundational import config_loader


DETAILS_SUBSET = []
LOGGER_SUBSET = [
    'format', 'station_name', 'logger_type', 'serial_num', 'OS_version',
    'program_name'
    ]
SITE_METADATA = config_loader.load_config_file_from_name(name='site_metadata')
logger = logging.getLogger(__name__)


def build_site_info(site: str, midnight: dt.datetime | None=None) -> dict:
    """
    Collate and write site information required for RTMC files.

    Args:
        site: name of site.

    Returns:
        None.

    """

    # Use today's midnight if not provided
    if midnight is None:
        midnight = dt.datetime.combine(dt.datetime.now().date(), dt.time())

    # Site metadata (maybe replace this with the glob_metadata module)
    try:
        site_info = SITE_METADATA[site].copy()
    except KeyError:
        raise KeyError(f"No metadata found for site '{site}'")

  # Add derived field
    try:
        commissioned = site_info.get("date_commissioned", "1900-01-01")
        site_info["start_year"] = str(
            dt.datetime.strptime(commissioned, "%Y-%m-%d").year
            )
    except Exception as e:
        logger.debug(
            "site_info_component_failed",
            extra={
                "site": site,
                "component": "start_year_derivation",
                "error_type": type(e).__name__,
                },
            )
        site_info["start_year"] = ""
  
    # Get sunrise / sunset info
    sun_info = _get_solar_info(
        site=site, site_info=site_info, date=midnight
        )
        
    # Check for fast data file
    fast_data_path = paths.get_local_stream_path(
        resource='raw_data', 
        stream='flux_fast',
        site=site,
        subdirs=['TOB3'],
        )    
    latest = get_most_recent_file(
        root=fast_data_path,
        pattern='TOB3*.dat',
        recursive=True
        )
    if latest is None:
        logger.debug(
            "site_info_component_missing",
            extra={
                "site": site,
                "component": "tob3_file",
            },
        )
        tob3_file = "No files"
    else:
        tob3_file = latest.name
    fast_file_info = {"10Hz_file": tob3_file}
  
    # Get flux logger info
    flux_file_path = None
    try:
        md_mngr = metadata.MetaDataManager(site=site)
        flux_file_path = md_mngr.data_path / md_mngr.flux_file

        logger_info = (
            md_mngr.get_file_attributes(file=md_mngr.flux_file)[LOGGER_SUBSET]
            .to_dict()
        )

    except Exception as e:
        logger.debug(
            "site_info_component_failed",
            extra={
                "site": site,
                "component": "logger_metadata",
                "error_type": type(e).__name__,
            },
        )
        logger_info = {k: "" for k in LOGGER_SUBSET}
        
    # Missing data info
    missing_info: dict = {}

    if flux_file_path is not None:
        try:
            missing_info = (
                fh.DataHandler(file=flux_file_path)
                .get_missing_records()
            )
            missing_info.pop("n_missing", None)

        except Exception as e:
            logger.debug(
                "site_info_component_failed",
                extra={
                    "site": site,
                    "component": "missing_data",
                    "error_type": type(e).__name__,
                },
            )
            missing_info = {}

    # Combine all
    combined_info = (
        site_info | sun_info | fast_file_info | logger_info | missing_info
        )
    combined_info = {
        k: ('' if v is None else v) for k, v in combined_info.items()
        }
    
    return combined_info

def _get_solar_info(site: str, site_info: dict, date: dt.datetime) -> dict:
    
    try:
        time_getter = TimeFunctions(
            lat=site_info["latitude"],
            lon=site_info["longitude"],
            elev=site_info["elevation"],
            date=date,
            )

        return {
            "sunrise": time_getter.get_next_sunrise().strftime("%H:%M"),
            "sunset": time_getter.get_next_sunset().strftime("%H:%M"),
            "time_zone": getattr(time_getter, "time_zone", ""),
            "UTC_offset": getattr(
                time_getter,
                "utc_offset",
                dt.timedelta(),
            ).total_seconds() / 3600,
            }

    except Exception as e:
        logger.debug(
            "site_info_component_failed",
            extra={
                "site": site,
                "component": "sun_times",
                "error_type": type(e).__name__,
                },
            )
        return {
            "sunrise": "",
            "sunset": "",
            "time_zone": "",
            "UTC_offset": "",
            }

def generate_site_info(
    *,
    run_id: str,
    site_list: list[str],
    ) -> dict:
    """
    Generate site_info.json for all sites.

    Returns:
        dict: per-site status results
    """

    start = time.perf_counter()

    logger.info(
        "site_info_task_start",
        extra={
            "run_id": run_id,
            "task_scope": "network",
            "site_count": len(site_list),
        },
    )

    data_list: list[dict] = []
    results: dict[str, dict] = {}

    for site in site_list:

        try:
            site_info = build_site_info(site=site)

            data_list.append({"site": site} | site_info)

            results[site] = {"status": "success"}

            logger.info(
                "site_info_site_processed",
                extra={
                    "run_id": run_id,
                    "site": site,
                    "status": "success",
                },
            )

        except KeyError as e:

            results[site] = {
                "status": "failure",
                "error_type": "metadata_missing",
                "error": str(e),
            }

            logger.warning(
                "site_info_site_skipped",
                extra={
                    "run_id": run_id,
                    "site": site,
                    "error_type": "metadata_missing",
                    "error": str(e),
                },
            )

        except Exception as e:

            results[site] = {
                "status": "failure",
                "error_type": type(e).__name__,
                "error": str(e),
            }

            logger.error(
                "site_info_site_failed",
                extra={
                    "run_id": run_id,
                    "site": site,
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

    # Infrastructure call (separated cleanly)
    output_path = paths.get_local_stream_path(
        resource="network",
        stream="status",
        file_name="site_info.json",
    )

    write_json_file(
        path=output_path,
        data=data_list,
        run_id=run_id,
        )

    duration = round(time.perf_counter() - start, 3)

    logger.info(
        "site_info_task_complete",
        extra={
            "run_id": run_id,
            "task_scope": "network",
            "sites_total": len(site_list),
            "sites_success": sum(r["status"] == "success" for r in results.values()),
            "sites_failure": sum(r["status"] == "failure" for r in results.values()),
            "duration_sec": duration,
            "output_path": str(output_path),
        },
    )

    return results