# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 12:34:58 2022

@author: jcutern-imchugh

This script fetches flux stations from TERN's SPARQL endpoint
"""

#------------------------------------------------------------------------------
### STANDARD IMPORTS ###
import datetime as dt
import ephem
import numpy as np
import pandas as pd
from pytz import timezone
import requests
from timezonefinder import TimezoneFinder
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CUSTOM IMPORTS #
from utils.configs_manager import PathsManager
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

    #--------------------------------------------------------------------------
    def _get_sunrise_sunset(
            self, site, date, state, which='next', utc=False, default_elev=100
            ):
        """
        Retrieve sunrise and sunset times from ephem.

        Parameters
        ----------
        site : str
            Site name.
        date : pydatetime
            The datetime for which to generate sunrise / sunset.
        state : str
            Determines whether to retrieve sunrise or sunset.
        which : str, optional
            Determines whether to retrieve previous or next sunrise or sunset.
        utc : bool, optional
            Determines whether to retrieve utc or local time. The default is
            False.
        default_elev : float or int, optional
            Elevation to use if the documented site elevation is absent. The
            default is 100.

        Raises
        ------
        KeyError
            Raised if 'state' parameter is not either sunrise or sunset, or
            'which' parameter is not either previous or next.
        TypeError
            Raised if documented latitude or longitude is absent.

        Returns
        -------
        pydatetime
            Requested sunrise or sunset time.

        """

        if not state in ['sunrise', 'sunset']:
            raise KeyError('"state" arg must be either sunrise or sunset')
        if not which in ['previous', 'next']:
            raise KeyError('"which" arg must be either last or next')

        obs = ephem.Observer()
        obs.lat = str(self.df.loc[site, 'latitude'])
        if np.isnan(obs.lat):
            raise TypeError('Site latitude is empty!')
        obs.long = str(self.df.loc[site, 'longitude'])
        if np.isnan(obs.lon):
            raise TypeError('Site latitude is empty!')
        obs.elev = self.df.loc[site, 'elevation']
        if np.isnan(obs.elev):
            print('Site latitude is empty!')
            obs.elev = default_elev
        obs.date = date
        sun = ephem.Sun()
        sun.compute(obs)
        utc_offset = dt.timedelta(hours=self.df.loc[site, 'UTC_offset'])

        if state == 'sunrise':
            if which == 'next':
                out_date = obs.next_rising(sun).datetime()
            else:
                out_date = obs.previous_rising(sun).datetime()
        else:
            if which == 'next':
                out_date = obs.next_setting(sun).datetime()
            else:
                out_date = obs.previous_setting(sun).datetime()
        if utc:
            return out_date
        return out_date + utc_offset
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_sunrise(self, site, date, which='previous', utc=False):
        """
        Get sunrise for the site and date.

        Parameters
        ----------
        site : str
            Site name.
        date : pydatetime
            The datetime for which to generate sunrise.
        which : str, optional
            Determines whether to retrieve previous or next sunrise.

        Returns
        -------
        pydatetime
            Requested sunrise.

        """

        return self._get_sunrise_sunset(
            site=site, date=date, state='sunrise', which=which, utc=utc
            )
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def get_sunset(self, site, date, which='next', utc=False):
        """
        Get sunset for the site and date.

        Parameters
        ----------
        site : str
            Site name.
        date : pydatetime
            The datetime for which to generate sunset.
        which : str, optional
            Determines whether to retrieve previous or next sunset.

        Returns
        -------
        pydatetime
            Requested sunset.

        """
        return self._get_sunrise_sunset(
            site=site, date=date, state='sunset', which=which, utc=utc
            )
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------

def get_acquired_sites():

    path =  paths.get_local_stream_path(resource='configs', stream='tasks')
    return path

def write_details_configs(site):

    pass