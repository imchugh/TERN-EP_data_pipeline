#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 11 15:49:52 2025

@author: imchugh
"""

import inspect

###############################################################################
### BEGIN TASK DECORATOR DEFINITION ###
###############################################################################

SITE_TASKS = {}
NETWORK_TASKS = {}

def register(func):

    # def decorator(func):
        # task_name = name or func.__name__
        # sig = inspect.signature(func)
        # params = list(sig.parameters.keys())
    params = list(inspect.signature(func).parameters.keys())
    # if kind == 'site' or (kind is None and params == ['site']):
    if params == ['site']:
        SITE_TASKS[func.__name__] = func
    else:
        NETWORK_TASKS[func.__name__] = func
    return func
    # return decorator

###############################################################################
### END TASK DECORATOR DEFINITION ###
###############################################################################