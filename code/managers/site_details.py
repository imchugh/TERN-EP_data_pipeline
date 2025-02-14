# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 12:34:58 2022

@author: jcutern-imchugh

This script fetches flux station details from TERN's SPARQL endpoint
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
import datetime as dt
import numpy as np
import pandas as pd
from pytz import timezone
import requests
from timezonefinder import TimezoneFinder
import yaml
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS #
from managers import paths
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

USERNAME = 'site-query'
PASSWORD = 'password123'
SPARQL_ENDPOINT = "https://graphdb.tern.org.au/repositories/knowledge_graph_core"
SPARQL_QUERY = """
PREFIX tern: <https://w3id.org/tern/ontologies/tern/>
PREFIX wgs: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX geosparql: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX tern-loc: <https://w3id.org/tern/ontologies/loc/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?id ?label ?fluxnet_id ?date_commissioned ?date_decommissioned ?latitude ?longitude ?elevation ?time_step ?freq_hz ?canopy_height ?soil ?tower_height ?vegetation
WHERE {
    ?id a tern:FluxTower ;
        rdfs:label ?label ;
        tern:fluxnetID ?fluxnet_id .

    OPTIONAL {
        ?id tern:dateCommissioned ?date_commissioned .
    }
    OPTIONAL {
        ?id tern:dateDecommissioned ?date_decommissioned .
    }
    OPTIONAL {
        ?id geosparql:hasGeometry ?geo .
        ?geo wgs:lat ?latitude ;
             wgs:long ?longitude .
        OPTIONAL {
            ?geo tern-loc:elevation ?elevation .
        }
    }
    OPTIONAL {
        ?id tern:hasAttribute ?time_step_attr .
        ?time_step_attr tern:attribute <http://linked.data.gov.au/def/tern-cv/ca60779d-4c00-470c-a6b6-70385753dff1> ;
            tern:hasSimpleValue ?time_step .
    }
    OPTIONAL {
        ?id tern:hasAttribute ?freq_hz_attr .
        ?freq_hz_attr tern:attribute <http://linked.data.gov.au/def/tern-cv/ce39d9fd-ef90-4540-881d-5b9e779d9842> ;
            tern:hasSimpleValue ?freq_hz .
    }
    OPTIONAL {
        ?id tern:hasAttribute ?canopy_height_attr .
        ?canopy_height_attr tern:attribute <http://linked.data.gov.au/def/tern-cv/c1920aed1295ee17a2aa05a9616e9b11d35e05b56f72ccc9a3748eb31c913551> ;
            tern:hasSimpleValue ?canopy_height .
    }
    OPTIONAL {
        ?id tern:hasAttribute ?soil_attr .
        ?soil_attr tern:attribute <http://linked.data.gov.au/def/tern-cv/ed2ebb7c-561a-4892-9662-3b3aaa9ec768> ;
            tern:hasSimpleValue ?soil .
    }
    OPTIONAL {
        ?id tern:hasAttribute ?tower_height_attr .
        ?tower_height_attr tern:attribute <http://linked.data.gov.au/def/tern-cv/d54e6e12-b9a6-42ac-ad2f-c56fb0d3e5d6> ;
            tern:hasSimpleValue ?tower_height_double .
    }
    OPTIONAL {
        ?id tern:hasAttribute ?vegetation_attr .
        ?vegetation_attr tern:attribute <http://linked.data.gov.au/def/tern-cv/1338fc29-53ef-4b27-8903-b824e973807a> ;
            tern:hasSimpleValue ?vegetation .
    }

    BIND(xsd:decimal(?tower_height_double) AS ?tower_height)

}
ORDER BY ?label
"""
# LIMIT 2
ALIAS_DICT = {'Alpine Peatland': 'Alpine Peat',
              'Aqueduct Snow Gum': 'SnowGum',
              'ArcturusEmerald': 'Emerald',
              'Calperum Chowilla': 'Calperum',
              'Dargo High Plains': 'Dargo',
              'Longreach Mitchell Grass Rangeland': 'Longreach',
              'Nimmo High Plains': 'Nimmo',
              'Samford Ecological Research Facility': 'Samford',
              'Silver Plain': 'SilverPlains',
              'Wellington Research Station Flux Tower': 'Wellington'
              }

HEADERS = {
    "content-type": "application/sparql-query",
    "accept": "application/sparql-results+json"
    }

DATE_VARS = ['date_commissioned', 'date_decommissioned']
NUM_VARS = [
    'latitude', 'longitude', 'elevation', 'time_step', 'freq_hz',
    'tower_height', 'UTC_offset', 'canopy_height'
    ]
DATE_FORMATS = ['%Y-%m-%d', '%d/%m/%Y']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PRIVATE FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_timezones(df):
    """Get the timezone (as region/city)"""

    tf = TimezoneFinder()
    tz_list = []
    for site in df.index:
        try:
            tz = tf.timezone_at(
                lng=df.loc[site, 'longitude'],
                lat=df.loc[site, 'latitude']
                )
        except ValueError:
            tz = np.nan
        tz_list.append(tz)
    return tz_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_UTC_offset(df):
    """Get the UTC offset (local standard time)"""

    offset_list = []
    date = dt.datetime.now()
    for site in df.index:
        try:
            tz_obj = timezone(df.loc[site, 'time_zone'])
            utc_offset = tz_obj.utcoffset(date)
            utc_offset -= tz_obj.dst(date)
            utc_offset = utc_offset.seconds / 3600
        except AttributeError:
            utc_offset = np.nan
        offset_list.append(utc_offset)
    return offset_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_str_2_date(date):
    """Return the passed date string in pydatetime format"""

    try:
        return dt.datetime.strptime(date, DATE_FORMATS[0]).date()
    except ValueError:
        return dt.datetime.strptime(date, DATE_FORMATS[1]).date()
    except TypeError:
        return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_date_2_str(date):

    try:
        return date.strftime(DATE_FORMATS[0])
    except AttributeError:
        return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_nums(float_str):
    """Return float or int as appropriate"""

    try:
        the_float = float(float_str)
        if int(the_float) == the_float:
            return int(the_float)
        return the_float
    except ValueError:
        return float_str
    except TypeError:
        return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_labels(label):
    """Format site name"""

    new_label = label.replace(' Flux Station', '')
    try:
        out_label = ALIAS_DICT[new_label]
    except KeyError:
        out_label = new_label
    return out_label.replace(' ', '')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### PRIVATE FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_df_from_db():
    """
    Query SPARQL endpoint for site data

    Raises
    ------
    RuntimeError
        If no response to request at server end.

    Returns
    -------
    df : pd.core.Frame.DataFrame
        Dataframe containing site details.

    """

    # Inits
    funcs_dict = (
        {'label': _parse_labels} |
        {var: _parse_str_2_date for var in DATE_VARS} |
        {var: _parse_nums for var in NUM_VARS}
        )

    # Query db
    response = requests.post(
        SPARQL_ENDPOINT, data=SPARQL_QUERY, headers=HEADERS,
        auth=(USERNAME, PASSWORD)
        )
    if response.status_code != 200:
        raise RuntimeError(response.text)
    json_dict = response.json()

    # Parse returned fields
    fields = json_dict['head']['vars']
    result_dict = {}
    for field in fields:
        temp_list = []
        for site in json_dict['results']['bindings']:
            try:
                temp_list.append(site[field]['value'])
            except KeyError:
                temp_list.append(None)
        try:
            result_dict[field] = [funcs_dict[field](x) for x in temp_list]
        except KeyError:
            result_dict[field] = temp_list
    names = result_dict.pop('label')

    # Build df
    df = pd.DataFrame(data=result_dict, index=names).sort_index()
    df.dropna(subset=['elevation', 'latitude', 'longitude'], inplace=True)
    df = df.assign(time_zone = _get_timezones(df))
    df = df.assign(UTC_offset = _get_UTC_offset(df))

    # Done
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_df_from_yml():

    # Inits
    funcs_dict = (
        {'label': _parse_labels} |
        {var: _parse_str_2_date for var in DATE_VARS} |
        {var: _parse_nums for var in NUM_VARS}
        )

    data = paths.get_internal_configs(config_name='site_metadata')

    rslt = {}
    for key, vars_dict in data.items():
        for this_variable in vars_dict.keys():
            try:
                func = funcs_dict[this_variable]
                vars_dict[this_variable] = func(vars_dict[this_variable])
            except KeyError:
                pass
        rslt[key] = vars_dict

    return pd.DataFrame(rslt).T
    # return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SiteDetailsManager():

    """Class to retrieve site data from SPARQL endpoint"""

    def __init__(self, use_local=False):

        if use_local:
            self.df = make_df_from_yml()
        else:
            self.df = make_df_from_db()

    #--------------------------------------------------------------------------
    def export_to_excel(self, output_path, operational_sites_only=True):

        """

        Parameters
        ----------
        path : str
            Output path for excel spreadsheet.
        operational_sites_only : Boolean, optional
            Drop non-operational sites. The default is True.

        Returns
        -------
        None.

        """

        if operational_sites_only:
            df = self.get_operational_sites()
        else:
            df = self.df
        df.to_excel(output_path, index_label='Site')
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_operational_sites(self, site_name_only=False):

        """
        Get the operational subset of sites.

        Returns
        -------
        pandas dataframe
            Dataframe containing information only for operational sites.

        """

        df = (
            self.df[pd.isnull(self.df.date_decommissioned)]
            .drop('date_decommissioned', axis=1)
            )
        if not site_name_only:
            return df
        return df.index.tolist()
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_single_site_details(self, site, field=None):

        if not field:
            return self.df.loc[site]
        return self.df.loc[site, field]
    #--------------------------------------------------------------------------

    # #--------------------------------------------------------------------------
    # def export_site_details_to_yml(self, site, output_path=None):

    #     # Set the output path
    #     if output_path is None:
    #         output_path = (
    #             paths.get_local_stream_path(
    #                 resource='network', stream='site_metadata'
    #                 ) /
    #             f'{site}_details.yml'
    #             )

    #     # Format the data
    #     site_data = (
    #         _format_site_data(site_data=self.df.loc[site].fillna('')).to_dict()
    #         )

    #     # Output the data
    #     with open(file=output_path, mode='w', encoding='utf-8') as f:
    #         yaml.dump(data=site_data.to_dict(), stream=f, sort_keys=False)
    # #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def export_all_details_to_yml(self, output_path, operational_sites_only=True):

        # Set the output path
        if output_path is None:
            output_path = (
                paths.get_local_stream_path(
                    resource='network', stream='site_metadata'
                    ) /
                'site_details.yml'
                )

        if operational_sites_only:
            site_list = self.get_operational_sites(site_name_only=True)
        else:
            site_list = self.df.index.tolist()

        site_data = {site: self.df.loc[site].to_dict() for site in site_list}
        with open(file=output_path, mode='w', encoding='utf-8') as f:
            yaml.dump(data=site_data, stream=f, sort_keys=False)
    #--------------------------------------------------------------------------
