# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 16:25:01 2024

@author: jcutern-imchugh
"""

import pandas as pd
import yaml

rename_dict = {
    'AH_IRGA_Av': 'AH_IRGA',
    'AH_2m': 'AH',
    'Ta_2m': 'Ta',
    'RH_2m': 'RH',
    'Sig_IRGA_Av': 'Sig_IRGA',
    'Wd_SONIC_Av': 'Wd',
    'Ws_SONIC_Av': 'Ws',
    'CO2_IRGA_Av': 'CO2c_IRGA'
    }

drop_list = [
    'Fg_1', 'Fg_2', 'Fg_3', 'Ts_1', 'Ts_2', 'Ts_3', 'Wd', 'Ws', 'CO2c_IRGA',
    'AH_IRGA', 'AH', 'Ta', 'RH', 'Sig_IRGA'
    ]

pfp_file = 'E:/Config_files/Variables/Calperum_pfp_variables.yml'
vis_file = 'E:/Config_files/Variables/Calperum_vis_variables.yml'

with open(pfp_file) as f:
    pfp_df = pd.DataFrame(yaml.safe_load(stream=f)).T
with open(vis_file) as f:
    vis_df = pd.DataFrame(yaml.safe_load(stream=f)).T

# Drop the variables that are already in the pfp dataframe under the same name
vis_df = vis_df.drop([x for x in vis_df.index if x in pfp_df.index])

# Drop the variables that are already in the pfp dataframe under different name
vis_df = vis_df.drop(drop_list)

# Add the variables that are missing in the vis dataframe
vis_df = vis_df.assign(
    height='20m', statistic_type='average'
    )

l = [x for x in vis_df.index if not x in pfp_df.index]

df = pd.concat([pfp_df, vis_df])

with open(
        file='E:/Scratch/Calperum_master_variables.yml', mode='w',
        encoding='utf-8'
        ) as f:
    yaml.dump(data=df.T.to_dict(), stream=f, sort_keys=False)