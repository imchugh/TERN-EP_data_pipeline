#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 25 16:00:26 2025

@author: imchugh
"""

from utils.metadata_handlers import MetaDataManager as mdm_old
from managers.metadata import MetaDataManager as mdm_new

def compare_files(site):

    site_mdm_old = mdm_old(site=site, variable_map='vis')
    site_mdm_new = mdm_new(site=site)

    old_not_new = [
        file for file in site_mdm_old.list_files()
        if not file in site_mdm_new.list_files()
        ]
    print(
        f'Files in the old manager only: {", ".join(old_not_new)}'
        )

    new_not_old = [
        file for file in site_mdm_new.list_files()
        if not file in site_mdm_old.list_files()
        ]
    print(
        f'Files in the new manager only: {", ".join(new_not_old)}'
        )

# CowBay