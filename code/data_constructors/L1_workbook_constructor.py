# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 12:51:45 2024

@author: imchugh
"""

import logging
import pandas as pd
import sys

import utils.metadata_handlers as mh
from file_handling import file_handler as fh
from paths import paths_manager as pm

logger = logging.getLogger(__name__)

def construct_L1_xlsx(site):

    logger.info('Getting variable information')
    md_mngr = mh.MetaDataManager(site=site, variable_map='vis')
    time_step = str(int(md_mngr.get_site_details().time_step)) + 'min'
    output_path = pm.get_local_stream_path(
        resource='homogenised_data',
        stream='xlsx',
        file_name=f'{site}_L1.xlsx'
        )

    logger.info('Opening excel for writing')
    with pd.ExcelWriter(path=output_path) as writer:

        for file in md_mngr.list_files():

            logger.info(f'Opening file {file}')

            input_path = md_mngr.data_path / file
            sheet_name = input_path.stem

            # Get file type, and disable file concatenation for all EddyPro files
            do_concat = True
            file_type = (
                md_mngr.get_file_attributes(file=file, return_field='format')
                )
            if file_type == 'EddyPro':
                do_concat = False

            # Get the handler
            handler = fh.DataHandler(file=input_path, concat_files=do_concat)

            # Write info line
            logger.info('Writing info line')
            (
                pd.DataFrame(handler.file_info.values())
                .T
                .to_excel(
                    writer,
                    sheet_name=sheet_name,
                    header=False,
                    index=False,
                    startrow=0,
                    engine='xlsxwriter'
                    )
                )

            # Write header lines
            logger.info('Writing header lines')
            (
                handler.get_conditioned_headers(output_format='TOA5')
                .reset_index()
                .T
                .to_excel(
                    writer,
                    sheet_name=sheet_name,
                    header=False,
                    index=False,
                    startrow=1,
                    engine='xlsxwriter'
                    )
                )

            # Write data
            logger.info('Writing data')
            (
                handler.get_conditioned_data(
                    resample_intvl=time_step,
                    output_format='TOA5'
                    )
                .to_excel(
                    writer,
                    sheet_name=sheet_name,
                    header=False,
                    index=False,
                    startrow=4,
                    na_rep='',
                    engine='xlsxwriter'
                    )
                )

    logger.info('Finished')

if __name__=="__main__":

    site=sys.argv[1]
    construct_L1_xlsx(site=site)