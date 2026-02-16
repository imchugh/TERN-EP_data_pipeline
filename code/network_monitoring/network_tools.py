#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  9 07:22:54 2026

@author: imchugh
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import logging
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------------------------------------------------------

from managers import paths

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN INITS ###
###############################################################################

logger = logging.getLogger(__name__)
SITE_CONNS = paths.get_internal_configs('vpn_ip')
LOGGER_IP = {'EC': '100', 'soil': '101'}
DEFAULT_LOGGER_PORT = 6785
VALID_HARDWARE = ['gateway', 'EC', 'soil']

###############################################################################
### END IMPORTS ###
###############################################################################



###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################


# -----------------------------------------------------------------------------
def is_reachable(host, ports, timeout=2.0) -> dict:
    """
    Try connecting to host on each port in order.

    Returns:
        (reachable: bool, port: int, latency_ms: int)

    Raises:
        RuntimeError if no ports are reachable
    """

    for port in ports:
        start = time.monotonic()

        try:
            logger.debug(
                'trying_port',
                extra={'host': host, 'port': port},
            )

            with socket.create_connection((host, port), timeout=timeout):
                latency_ms = int((time.monotonic() - start) * 1000)

                logger.debug(
                    'port_reachable',
                    extra={
                        'host': host,
                        'port': port,
                        'latency_ms': latency_ms,
                    },
                )

                return True, port, latency_ms

        except OSError as exc:
            logger.debug(
                'port_unreachable',
                extra={
                    'host': host,
                    'port': port,
                    'error': str(exc),
                },
            )

    raise RuntimeError('no ports reachable')
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def scan_site(site: dict, timeout=2.0) -> dict:
    """
    Check reachability for a single site definition.

    Args:
        site (dict): must contain key:value pairs for 'ip' and 'ports'

    Returns:
        dict with reachability info
    """

    logger.debug(
        'scan_site_start',
        extra={'ip': site['ip'], 'ports': site['ports']},
    )

    ok, port, latency_ms = is_reachable(
        site['ip'],
        site['ports'],
        timeout,
    )

    logger.debug(
        'scan_site_complete',
        extra={
            'ip': site['ip'],
            'port': port,
            'latency_ms': latency_ms,
        },
    )

    return {
        'reachable': ok,
        'port': port,
        'latency_ms': latency_ms,
    }
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def scan_site_by_name(
    site_name: str, hardware: str='gateway', timeout: int=2.0, 
    ports_override: list = None
    ) -> dict:
    """
    Check reachability for a site identified by name.

    Args:
        site_name: name of site to scan.
        timeout (optional): DESCRIPTION. Defaults to 2.0.
        ports_override (optional): pass a list of alternate ports to override 
        config defaults. Defaults to None (i.e. use configs).

    Returns:
        results for passed site.

    """

    # Grab site-level configs and convert if not gateway
    cfg = SITE_CONNS[site_name]
    if not hardware == 'gateway':
        cfg['ip'] = _get_logger_ip(gateway_ip=cfg['ip'], logger_type=hardware)
        cfg['ports'] = [DEFAULT_LOGGER_PORT]
    
    logger.debug(
        'scan_site_by_name',
        extra={'site': site_name},
    )

    site = {
        'ip': cfg['ip'],
        'ports': ports_override or cfg['ports'],
    }

    return scan_site(site, timeout)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def scan_network(
        site_list: list=None, hardware: str='gateway', timeout: int=2.0, 
        max_workers: int=10
        ) -> dict:
    """
    Scan multiple sites concurrently.

    Args:
        site_list (list): names of sites to scan. Defaults to None.
        timeout (optional): Timeout in seconds. Defaults to 2.0.
        max_workers (optional): Number of worker threads. Defaults to 10.

    Returns:
        dict containing results for all passed sites.

    """

    if site_list is None:
        site_list = SITE_CONNS.keys()

    logger.debug(
        'scan_network_start',
        extra={
            'site_count': len(site_list),
            'max_workers': max_workers
        }
    )

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_name = {
            pool.submit(
                scan_site_by_name,
                name,
                hardware,
                timeout,
            ): name
            for name in site_list
        }

        for future in as_completed(future_to_name):
            name = future_to_name[future]

            try:
                results[name] = future.result()
            except Exception as exc:
                # Unreachable site
                logger.debug(
                    'site_check_exception',
                    extra={
                        'site': name,
                        'error': str(exc),
                    },
                )

                results[name] = {
                    'reachable': False,
                    'port': None,
                    'latency_ms': None,
                }

    # Spit out the logger
    logger.debug(
        'scan_network_complete',
        extra={'site_count': len(results)},
    )

    return results
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
def _get_logger_ip(gateway_ip, logger_type='EC'):
    
    host_addr = LOGGER_IP[logger_type]
    return f'192.168.{str(gateway_ip.split('.')[-1])}.{host_addr}'
# -----------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################
