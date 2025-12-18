# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 12:34:58 2022

@author: jcutern-imchugh

This script fetches flux station details from TERN's SPARQL endpoint
"""

###############################################################################
### BEGIN IMPORTS ###
###############################################################################

import datetime as dt
import numpy as np
import pandas as pd
import pathlib
from pytz import timezone
import requests
from timezonefinder import TimezoneFinder
import yaml

# -----------------------------------------------------------------------------

from managers import paths
from managers import dereference

###############################################################################
### END IMPORTS ###
###############################################################################


###############################################################################
### BEGIN INITS ###
###############################################################################

# Get creds
creds = paths.get_local_config_file(config_stream='secrets')['SITE_DETAILS']
USERNAME = creds['USERNAME']
PASSWORD = creds['PASSWORD']

# Define function to import yml


def _load_yml(file):

    with open(file) as f:
        return yaml.safe_load(stream=f)


# Set consts
CONFIGS = _load_yml(
    pathlib.Path(__file__).parents[1] / 'configs' / 'sparql_queries.yml'
)
QUERY = CONFIGS['queries']
SPARQL_ENDPOINT = CONFIGS['sparql_endpoint']
HEADERS = CONFIGS['query_headers']

# Define aliases
ALIAS_DICT = {
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

# Set data types
DATA_DTYPES = {
    'id': 'string', 'fluxnet_id': 'string', 'date_commissioned': 'datetime64[ns]',
    'date_decommissioned': 'datetime64[ns]', 'latitude': float,
    'longitude': float, 'elevation': float, 'time_step': float,
    'freq_hz': float, 'soil': 'string', 'tower_height': float, 'vegetation': 'string',
    'canopy_height': 'string', 'time_zone': 'string', 'UTC_offset': float
}
DATE_VARS = ['date_commissioned', 'date_decommissioned']
INT_VARS = ['time_step', 'freq_hz']
tf = TimezoneFinder()

###############################################################################
### END INITS ###
###############################################################################


###############################################################################
### BEGIN FUNCTIONS ###
###############################################################################

# -----------------------------------------------------------------------------
def list_flux_tower_predicates() -> dict:
    """
    Get the UIDs of the RDF graph predicates for flux towers.

    Returns:
        dictionary mapping common names (keys) to UIDs (values).

    """

    rslt = do_query(query=QUERY['list_predicates'])
    bindings = rslt['results']['bindings']
    return {
        uid['predicate']['value'].split('/')[-1]:
            uid['predicate']['value'] for uid in bindings
    }
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def list_flux_tower_attrs() -> dict:
    """
    Get the UIDs of the RDF graph nested attrs for flux towers.

    Args:
        do_format (optional): return the data as a type-formatted and
        validated site-indexed metadata dataframe. Defaults to False.

    Returns:
        dictionary mapping common names (keys) to UIDs (values).

    """

    rslt = do_query(query=QUERY['get_attributes'])
    bindings = rslt["results"]["bindings"]
    uuid_list = [b["attr_uuid"]["value"] for b in bindings]
    return {
        value: key for key, value in
        dereference.dereference_labels(uris=uuid_list).items()
    }
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def get_flux_tower_fields(
        query: str = 'operational', do_format: bool = False
) -> dict | pd.DataFrame:
    """
    Get the values for the operationally-required global metadata fields from
    the RDF graph.

    Args:
        fields (TYPE, optional): DESCRIPTION. Defaults to 'operational'.

    Returns:
        data: type-formatted and validated site-indexed metadata dataframe.

    """

    # Set query
    mode = 'get_operational'
    if query == 'extended':
        mode = 'get_extended'

    # Query and extract the data
    bindings = do_query(query=QUERY[mode])['results']['bindings']
    rslt = []
    for data_set in bindings:
        rslt.append(
            {key: sub_dict['value'] for key, sub_dict in data_set.items()}
        )
    if not do_format:
        return rslt

    # Construct and index the dataframe
    data = pd.DataFrame(rslt, dtype='O')
    data.index = pd.Index(
        data=[_parse_labels(label) for label in data.label],
        name='Site'
    )
    data = data.drop('label', axis=1)

    # Format the data
    data = _format_data(data=data)

    # Create new variables and return
    data['time_zone'] = data.apply(_get_timezone, axis=1)
    data['UTC_offset'] = data.time_zone.apply(_get_UTC_offset)

    return data
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def get_flux_tower_geometry(do_format: bool = False) -> dict | pd.DataFrame:
    """
    Get lat / long / elevation nested attributes.

    Args:
        do_format (optional): return the data as a type-formatted and
        validated site-indexed metadata dataframe. Defaults to False.

    Returns:
        TYPE: DESCRIPTION.

    """

    rslt = do_query(query=QUERY['get_geometry'])
    bindings = rslt['results']['bindings']
    site_list, values_list = [], []
    for b in bindings:
        vals = {'latitude': None, 'longitude': None, 'elevation': None}
        site = b['label']['value']
        site_list.append(site)
        for var in ['latitude', 'longitude', 'elevation']:
            if var in b:
                vals[var] = b[var]['value']
        values_list.append(vals)
    if not do_format:
        return values_list
    return pd.DataFrame(data=values_list, index=site_list, dtype=float)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def do_query(query: str) -> dict:
    """
    Run the query.

    Args:
        query: sparql query string.

    Returns:
        dict.

    """

    response = requests.post(
        SPARQL_ENDPOINT,
        data=query,
        headers=HEADERS,
        auth=(USERNAME, PASSWORD),
        timeout=30
    )
    response.raise_for_status()
    return response.json()
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def _parse_labels(label: str) -> str:
    """
    Rebuild labels to match standard site names.

    Args:
        label: the site name label to edit.

    Returns:
        the edited site name label.

    """

    new_label = label.replace(' Flux Station', '')
    try:
        out_label = ALIAS_DICT[new_label]
    except KeyError:
        out_label = new_label
    return out_label.replace(' ', '')
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def _get_timezone(row: pd.Series) -> str:
    """
    Use lat / long to find timezone name.

    Args:
        row: series containing latitude and longitude.

    Returns:
        timezone name.

    """

    try:
        return tf.timezone_at(lng=row['longitude'], lat=row['latitude'])
    except ValueError:
        return np.nan
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def _get_UTC_offset(time_zone: str) -> float:
    """
    Find the offset in decimal hours from UTC.

    Args:
        time_zone: name of time zone (format: `city/country`).

    Returns:
        the offset.

    """

    date = dt.datetime.now()
    try:
        tz_obj = timezone(time_zone)
        utc_offset = tz_obj.utcoffset(date)
        utc_offset -= tz_obj.dst(date)
        utc_offset = utc_offset.seconds / 3600
    except AttributeError:
        utc_offset = np.nan
    return utc_offset
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def import_data_from_yml() -> pd.DataFrame:
    """
    Import the site metadata from the local yml file (written from the db).

    Returns:
        the metadata.

    """

    return _format_data(
        data=(
            pd.DataFrame(paths.get_internal_configs(
                config_name='site_metadata')
            )
        )
        .T
    )
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------


def _format_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Format the data.

    Args:
        data: the unformatted data.

    Returns:
        the formatted data.

    """

    # Handle possible missing variables
    types = {
        key: value for key, value in DATA_DTYPES.items()
        if key in data.columns
    }

    # For string variables, replace missing with ''
    str_types = {
        key: value for key, value in types.items() if value == 'string'
    }
    data[list(str_types.keys())] = (
        data[str_types.keys()].replace((None, np.nan), '')
    )

    # Format and return
    return (
        data
        .astype(types)
        .astype({var: 'Int64' for var in INT_VARS})
    )
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def export_data_to_yml(
        output_path: pathlib.Path | str = None, operational_sites_only: bool = True
):
    """
    Collect the data from the db and spit out to yml.

    Args:
        output_path (optional): output path for the yml file. Defaults to None.
        operational_sites_only (optional): only output the operational sites.
        Defaults to True.

    Returns:
        None.

    """

    # Set the output path
    if output_path is None:
        output_path = (
            paths.get_local_stream_path(
                resource='network', stream='site_metadata'
            ) /
            'site_details.yml'
        )

    # Get the data and truncate to operational sites only
    data = get_flux_tower_fields(do_format=True)
    drop_vars = []
    if operational_sites_only:
        data = (
            data[pd.isnull(data.date_decommissioned)]
        )
        drop_vars = ['date_decommissioned']

     # Convert date variables back to string
    data = data.astype({label: 'str' for label in DATE_VARS})

    # Drop date decommissioned if only operational sites returned
    data = data.drop(drop_vars, axis=1)

    # Convert null values to None, which should be null in yaml
    site_data = {
        site: _format_canopy_height(
            in_dict=data.loc[site].replace((np.nan, 'nan', ''), None).to_dict()
        )
        for site in data.index
    }

    # Output to yaml
    with open(file=output_path, mode='w', encoding='utf-8') as f:
        yaml.dump(data=site_data, stream=f, sort_keys=False)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def _format_canopy_height(in_dict):
    """
    Convert canopy heights to floats where there are valid data.

    Args:
        in_dict: dictionary containing canopy_height as str.

    Returns:
        Dictionary containing canopy height as float.

    """

    # Try to convert canopy height to a float.
    try:
        in_dict.update({'canopy_height': float(in_dict['canopy_height'])})
    except (ValueError, TypeError):
        pass
    return in_dict
# -----------------------------------------------------------------------------

###############################################################################
### END FUNCTIONS ###
###############################################################################


###############################################################################
### BEGIN CLASSES ###
###############################################################################

# -----------------------------------------------------------------------------


class GlobalMetadataManager():
    """Class to retrieve site data from SPARQL endpoint"""

    # -------------------------------------------------------------------------
    def __init__(self, df: pd.DataFrame) -> None:
        """
        Populate class with database content.

        Args:
            use_local (optional): if true, populate from local yml file.
            If false, populate from remote db. Defaults to False.

        Returns:
            None.

        """

        self.df = df
    # --------------------------------------------------------------------------

    # --------------------------------------------------------------------------
    def get_operational_sites(
            self, site_name_only: bool = False
    ) -> pd.DataFrame | list:
        """
        Get the operational subset of sites.

        Args:
            site_name_only (optional): if True, returns only the list of |
            operational sites (no metadata). Defaults to False.

        Returns:
            dataframe containing operation site data or list of operation sites.

        """

        df = (
            self.df[pd.isnull(self.df.date_decommissioned)]
            .drop('date_decommissioned', axis=1)
        )
        if not site_name_only:
            return df
        return df.index.tolist()
    # --------------------------------------------------------------------------

    # --------------------------------------------------------------------------
    def get_single_site_details(
            self, site: str, field: str = None
    ) -> pd.Series | str:
        """
        Return fields for a single site.

        Args:
            site: name of site.
            field (optional): name of field to return. Defaults to None.

        Returns:
            series of fields as series or individual field as str.

        """

        # Format the output data
        if not field:
            return self.df.loc[site]
        return self.df.loc[site, field]
    # --------------------------------------------------------------------------

    # --------------------------------------------------------------------------
    def get_single_site_details_as_dict(
        self, site: str, field: str = None
    ) -> pd.Series | str:
        """


        Args:
            site (str): DESCRIPTION.
            field (str, optional): DESCRIPTION. Defaults to None.

        Returns:
            rslt (TYPE): DESCRIPTION.

        """

        # Get the data series and convert to dict, and subset if field requested
        rslt = self.df.loc[site].to_dict()
        if field:
            rslt = {field: rslt.pop(field)}

        try:
            rslt['canopy_height'] = float(rslt['canopy_height'])
        except KeyError:
            pass
        except ValueError:
            pass

        for date_var in ['date_commissioned', 'date_decommissioned']:
            try:
                rslt[date_var] = rslt[date_var].to_pydatetime()
            except KeyError:
                continue

        if len(rslt) == 1:
            return list(rslt.values())[0]
        return rslt
    # --------------------------------------------------------------------------

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------


def get_metadata_manager(*, source: str, **kwargs) -> GlobalMetadataManager:
    """
    Factory function to return the class which is always the same but depend on 
    the input method.

    Args:
        source: .
        **kwargs: just used to pass in the mode for the rdf query 
        (`operational` or `extended`).

    Raises:
        ValueError: raised if unknown source passed.

    Returns:
        GlobalMetadataManager class.

    """

    if source == "rdf":
        df = get_flux_tower_fields(query=kwargs['query'], do_format=True)
    elif source == "yml":
        file = (
            pathlib.Path(__file__).parents[1] / 'configs' / 'site_metadata.yml'
        )
        df = _format_data(
            data=pd.DataFrame.from_dict(
                data=_load_yml(file=file),
                orient='index'
            )
        )
    else:
        raise ValueError("Unknown source")

    return GlobalMetadataManager(df)
# -----------------------------------------------------------------------------

###############################################################################
### END CLASSES ###
###############################################################################
