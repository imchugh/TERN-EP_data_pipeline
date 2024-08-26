# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 14:25:12 2024

@author: jcutern-imchugh
"""

import sys
sys.path.append('E:/Code/TERN_EP_data_pipeline/code/')

import main

def test_something():

    task = sys.argv[1]
    try:
        site=sys.argv[2]
    except IndexError:
        site=None
    main.run_task(task=task, site=site)


# # Load task configuration file
# task_configs_path = paths.get_local_stream_path(
#     resource='configs',
#     stream='tasks'
#     )
# with open(task_configs_path) as f:
#     _task_configs = yaml.safe_load(stream=f)
# NETWORK_TASKS = _task_configs['generic_tasks']
# SITE_TASKS = pd.DataFrame(_task_configs['site_tasks']).T


# ###############################################################################
# ### END TASK MANAGEMENT FUNCTIONS ###
# ###############################################################################

#------------------------------------------------------------------------------
if __name__=='__main__':

    test_something()