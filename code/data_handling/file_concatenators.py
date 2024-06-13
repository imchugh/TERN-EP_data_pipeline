# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 13:14:17 2023

To do:
    - add file type check to file concatenator
    - fix path formatting of master versus merge file in the concatenator
    - create a report subclass to be embedded in the concatenator, which can be
      handed off to the merger so an independent call to the file write is not
      required

@author: jcutern-imchugh
"""

import pandas as pd

import data_handling.file_io as io

UNIT_ALIASES = {
    'degC': ['C'],
    'n': ['arb', 'samples'],
    'arb': ['n', 'samples'],
    'samples': ['arb', 'n'],
    'm^3/m^3': ['fraction']
    }

#------------------------------------------------------------------------------
# Merging / concatenation classes #
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class FileConcatenator():
    """Class to allow multiple file merges to a master file"""

    def __init__(self, master_file, concat_list, file_type):
        """
        Get merge reports as hidden attributes.

        Parameters
        ----------
        master_file : str or pathlib.Path
            Absolute path to master file.
        concat_list : list
            List of absolute path to files to be concatenated with the master
            file.
        file_type : str
            The type of file (must be either "TOA5" or "EddyPro")

        Returns
        -------
        None.

        """

        self.master_file = master_file
        self.concat_list = concat_list
        self.file_type = file_type
        self.file_info = io.get_file_type_configs(file_type=file_type)
        reports = [
            FileMergeAnalyser(
                master_file=master_file,
                merge_file=file,
                file_type=file_type
                )
            .get_merge_report()
            for file in self.concat_list
            ]
        self.legal_list = [
            report['merge_file'] for report in reports if
            report['file_merge_legal']
            ]
        self.concat_reports = reports
        self.alias_maps = {
            report['merge_file']: report['aliased_units'] for report in
                reports
                }

    #--------------------------------------------------------------------------
    def get_concatenated_data(self):
        """
        Concatenate the data from the (legal) files in the concatenation list.

        Returns
        -------
        pd.core.frame.DataFrame
            The data, with columns order enforced from header (in case pandas
            handles ordering differently for the data versus header).

        """

        df_list = [io.get_data(file=self.master_file, file_type=self.file_type)]
        for file in self.legal_list:
            df_list.append(
                io.get_data(file=file, file_type=self.file_type)
                .rename(self.alias_maps[file], axis=1)
                )
        ordered_vars = self.get_concatenated_header().index.tolist()
        return (
            pd.concat(df_list)
            [ordered_vars]
            .sort_index()
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_concatenated_header(self):
        """
        Concatenate the headers from the (legal) files in the concatenation list.

        Returns
        -------
        pd.core.frame.DataFrame
            The concatenated headers.

        """

        df_list = [
            io.get_header_df(file=self.master_file, file_type=self.file_type)
            ]
        for file in self.legal_list:
            df_list.append(
                io.get_header_df(file=file, file_type=self.file_type)
                .rename(self.alias_maps[file])
                )
        df = pd.concat(df_list)
        return df[~df.index.duplicated()]
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_concatenation_report(self, as_text=False):
        """
        Get the concatenation report.

        Parameters
        ----------
        as_text : bool
            Return the reports as a list of strings ready for output to file.

        Returns
        -------
        None.

        """

        if not as_text:
            return self.concat_reports

        line_list = [
            f'Merge report for {self.file_type} master file '
            f'{str(self.master_file)}\n'
            ]

        for report in self.concat_reports:
            line_list.extend(
                _get_results_as_txt(report) + ['\n']
                )

        if as_text:
            return line_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def write_concatenation_report(self, abs_file_path):
        """
        Write the concatenation report.

        Parameters
        ----------
        abs_file_path : str or pathlib.Path
            The file (including absolute path) to the file to write the report to.

        Returns
        -------
        None.

        """

        _write_text_to_file(
            line_list=self.get_concatenation_report(as_text=True),
            abs_file_path=abs_file_path
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class FileMergeAnalyser():
    """Analyse compatibility of merge between master and merge file."""

    def __init__(self, master_file, merge_file, file_type):
        """
        Initialise master, merge and file type parameters.

        Parameters
        ----------
        master_file : str or pathlib.Path
            Absolute path to master file.
        merge_file : str or pathlib.Path
            Absolute path to merge file.
        file_type : str
            The type of file (must be either "TOA5" or "EddyPro")

        Returns
        -------
        None.

        """

        if master_file == merge_file:
            raise RuntimeError('Master and merge file are the same!')
        self.master_file = master_file
        self.merge_file = merge_file
        self.file_type = file_type

    #--------------------------------------------------------------------------
    def compare_variables(self):
        """
        Check which variables are common or held in one or other of master and
        merge file (merge is deemed illegal if there are no common variables).

        Returns
        -------
        dict
            Analysis results and boolean legality.

        """

        master_df = io.get_header_df(
            file=self.master_file, file_type=self.file_type
            )
        merge_df = io.get_header_df(
            file=self.merge_file, file_type=self.file_type
            )
        common = list(set(master_df.index).intersection(merge_df.index))
        return {
            'common_variables': common,
            'variable_merge_legal': len(common) > 0,
            'master_only': list(
                set(master_df.index.tolist()) - set(merge_df.index.tolist())
                ),
            'merge_only': list(
                set(merge_df.index.tolist()) - set(master_df.index.tolist())
                )
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_units(self):
        """
        Compare units header line (merge is deemed illegal if there are unit
        changes).

        Returns
        -------
        dict
            return a dictionary with the mismatched header elements
            (key='units_mismatch'), any aliased units(key='aliased units')
            and boolean legality (key='units_merge_legal').

        """

        # Subset the analysis for common variables, and build comparison df
        common_vars = (
            self.compare_variables()['common_variables']
            )
        compare_df = pd.concat(
            [(io.get_header_df(file=self.master_file, file_type=self.file_type)
              .rename({'units': 'master'}, axis=1)
              .loc[common_vars, 'master']
              ),
              (io.get_header_df(file=self.merge_file, file_type=self.file_type)
              .rename({'units': 'merge'}, axis=1)
              .loc[common_vars, 'merge']
              )], axis=1
            )

        # Get mismatched variables and check whether they are just aliases
        # (in which case merge is deemed legal, and alias mapped to master)
        mismatch_df = compare_df[compare_df['master']!=compare_df['merge']]
        mismatch_list, alias_dict = [], {}
        units_merge_legal = True
        for variable in mismatch_df.index:
            master_units = mismatch_df.loc[variable, 'master']
            merge_units = mismatch_df.loc[variable, 'merge']
            try:
                assert merge_units in UNIT_ALIASES[master_units]
                alias_dict.update({merge_units: master_units})
            except (KeyError, AssertionError):
                mismatch_list.append(variable)
                units_merge_legal = False

        # Return the result
        return {
            'units_mismatch': mismatch_list,
            'aliased_units': alias_dict,
            'units_merge_legal': units_merge_legal
            }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_interval(self):
        """
        Cross-check concatenation file has same frequency as master (merge is
        deemed illegal if not).

        Returns
        -------
        dict
            Dictionary with indication of whether merge intervals are same
            (legal) or otherwise (illegal).

        """

        return {
            'interval_merge_legal':
                io.get_file_interval(
                    file=self.master_file, file_type=self.file_type
                    ) ==
                io.get_file_interval(
                    file=self.merge_file, file_type=self.file_type
                    )
                }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def compare_dates(self):
        """
        Check that the merge file contains unique dates (merge is deemed
        illegal if there are none).

        Returns
        -------
        dict
            Dictionary with indication of whether unique dates exist (legal)
            or otherwise (illegal).

        """

        return {
            'date_merge_legal':
                len(
                    set(io.get_dates(
                        file=self.master_file, file_type=self.file_type
                        )) -
                    set(io.get_dates(
                        file=self.merge_file, file_type=self.file_type
                        ))
                    ) > 0
                }
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_merge_report(self, as_text=False, abs_file_path=None):
        """
        Get the merge report.

        Parameters
        ----------
        as_text : bool
            Return the reports as a list of strings ready for output to file.
        abs_file_path : str or pathlib.Path, optional
            Absolute file path to which to output the data. The default is None.

        Returns
        -------
        None.

        """

        results = (
            {
                'master_file': str(self.master_file),
                'merge_file': str(self.merge_file)
                } |
            self.compare_dates() |
            self.compare_interval() |
            self.compare_variables() |
            self.compare_units()
            )
        results['file_merge_legal'] = all(
            results[key] for key in results.keys() if 'legal' in key
            )

        if not any([as_text, abs_file_path]):
            return results

        line_list = (
            [f'Merge report for {self.file_type} master file '
             f'{self.master_file}\n'] +
            _get_results_as_txt(results=results)
            )

        if as_text:
            return line_list

        if abs_file_path:

            _write_text_to_file(
                line_list=line_list,
                abs_file_path=abs_file_path
                )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class ConcatenationReporter():

    #--------------------------------------------------------------------------
    def __init__(self, master_file, concat_list, file_type=None):

        reports = [
            FileMergeAnalyser(
                master_file=master_file,
                merge_file=file,
                file_type=file_type
                )
            .get_merge_report()
            for file in self.concat_list
            ]
        self.legal_list = [
            report['merge_file'] for report in reports if
            report['file_merge_legal']
            ]
        self.illegal_list = []
        pass
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_results_as_txt(results):

    return [
        f'Merge file: {results["merge_file"]}',
        f'Merge legal? -> {str(results["file_merge_legal"])}',
        f'  - Date merge legal? -> {str(results["date_merge_legal"])}',
        '  - Interval merge legal? -> '
        f'{str(results["interval_merge_legal"])}',
        '  - Variable merge legal? -> '
        f'{str(results["variable_merge_legal"])}',
        '    * Variables contained only in master file -> '
        f'{results["master_only"]}',
        '    * Variables contained only in merge file -> '
        f'{results["merge_only"]}',
        f'  - Units merge legal? -> {str(results["units_merge_legal"])}',
        f'    * Variables with aliased units -> {results["aliased_units"]}',
        '    * Variables with mismatched units -> '
        f'{results["units_mismatch"]}',
        ]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _write_text_to_file(line_list, abs_file_path):

    with open(abs_file_path, 'w') as f:
        for line in line_list:
            f.write(f'{line}\n')
#------------------------------------------------------------------------------