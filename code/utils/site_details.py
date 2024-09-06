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
from utils.paths_manager import PathsManager
from utils import configs_getters as cg
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

SPARQL_ENDPOINT = "https://graphdb.tern.org.au/repositories/knowledge_graph_core"
SPARQL_QUERY = """
PREFIX tern: <https://w3id.org/tern/ontologies/tern/>
PREFIX wgs: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX geosparql: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX tern-loc: <https://w3id.org/tern/ontologies/loc/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?id ?label ?fluxnet_id ?date_commissioned ?date_decommissioned ?latitude ?longitude ?elevation ?time_step ?freq_hz
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
              'Wellington Research Station Flux Tower': 'Wellington'
              }

HEADERS = {
    "content-type": "application/sparql-query",
    "accept": "application/sparql-results+json"
    }

paths = PathsManager()
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
def _parse_dates(date):
    """Return the passed date string in pydtetime format"""

    DATE_FORMATS = ['%Y-%m-%d', '%d/%m/%Y']
    try:
        return dt.datetime.strptime(date, DATE_FORMATS[0])
    except ValueError:
        return dt.datetime.strptime(date, DATE_FORMATS[1])
    except TypeError:
        return None
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _parse_floats(float_str):
    """Return float or int as appropriate"""

    try:
        the_float = float(float_str)
        if int(the_float) == the_float:
            return int(the_float)
        return the_float
    except TypeError:
        return np.nan
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
def make_df():
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

    funcs_dict = {'label': _parse_labels,
                  'date_commissioned': _parse_dates,
                  'date_decommissioned': _parse_dates,
                  'latitude': _parse_floats,
                  'longitude': _parse_floats,
                  'elevation': _parse_floats,
                  'time_step': _parse_floats}

    response = requests.post(SPARQL_ENDPOINT, data=SPARQL_QUERY, headers=HEADERS)
    if response.status_code != 200:
        raise RuntimeError(response.text)
    json_dict = response.json()
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
    df = pd.DataFrame(data=result_dict, index=names)
    df.dropna(subset=['elevation', 'latitude', 'longitude'], inplace=True)
    df = df.assign(time_zone = _get_timezones(df))
    df = df.assign(UTC_offset = _get_UTC_offset(df))
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class SiteDetails():

    """Class to retrieve site data from SPARQL endpoint"""

    def __init__(self, use_alias=True):

        self.df = make_df()

    #--------------------------------------------------------------------------
    def export_to_excel(self, path, operational_sites_only=True):

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
        df.to_excel(path, index_label='Site')
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

#------------------------------------------------------------------------------

def write_site_details_to_configs(site=None):

    # Set the output path
    out_path = paths.get_local_stream_path(
        resource='configs', stream='site_details'
        )

    # Get the data
    df = make_df()

    # Get the site list
    if not site is None:
        site_list = (cg.get_task_configs()['site_tasks']).keys()
    else:
        site_list = [site]

    # Iterate over site list
    for site in site_list:

        try:

            site_data = _format_site_data(site_data=df.loc[site].fillna(''))

            file = out_path / f'{site}_details.yml'

            # Output
            with open(file=file, mode='w', encoding='utf-8') as f:
                yaml.dump(data=site_data.to_dict(), stream=f, sort_keys=False)

        except KeyError:

            continue


def _format_site_data(site_data):

    for entry in ['date_commissioned', 'date_decommissioned']:
        try:
            site_data[entry] = site_data[entry].strftime('%Y-%m-%d')
        except AttributeError:
            site_data[entry] = None
    for entry in ['freq_hz', 'time_step']:
        try:
            site_data[entry] = int(site_data[entry])
        except ValueError:
            pass
    dtypes_dict = {
        'latitude': 'float64',
        'longitude': 'float64',
        'elevation': 'float64',
        'UTC_offset': 'float64'
        }
    for key, value in dtypes_dict.items():
        site_data[key] = site_data[key].astype(value)
    return site_data
