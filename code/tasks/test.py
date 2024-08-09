# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 09:40:12 2024

@author: jcutern-imchugh
"""

import logging
import yaml

import network_monitoring.network_status as ns

logger = logging.getLogger(__name__)

with open('logger_configs.yml') as f:
    rslt = yaml.safe_load(stream=f)
    rslt['handlers']['file']['filename'] = 'E:/Scratch/loggety_logs.log'
    logging.config.dictConfig(rslt)

logger.info('Writing network status!')
ns.write_status_xlsx()
logger.info('Wrote network status!')
