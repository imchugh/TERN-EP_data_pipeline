#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 27 15:34:47 2026

@author: imchugh
"""

from infrastructure import paths
from infrastructure.external_io import post
from services.foundational import config_loader

cred_path = paths.get_local_stream_path(resource='configs', stream='secrets')
creds = config_loader.load_config_file(file=cred_path)['SITE_DETAILS']
USERNAME = creds['USERNAME']
PASSWORD = creds['PASSWORD']
SPARQL_CONFIGS = config_loader.load_config_file_from_name(name='sparql_queries')

def run_query(query: str) -> dict:
    response = post(
        SPARQL_CONFIGS['sparql_endpoint'],
        data={'query': query},
        headers={
            "Accept": "application/sparql-results+json"
        },#SPARQL_CONFIGS['query_headers'],
        auth=(USERNAME, PASSWORD),
        timeout=60,
    )
    return response.json()

spq = """
PREFIX tern: <https://w3id.org/tern/ontologies/tern/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>

SELECT ?id ?label ?commissioned ?geom
WHERE {
  ?id a tern:Platform ;
      rdfs:label ?label ;
      geo:hasGeometry ?geom .

  OPTIONAL { ?id tern:dateCommissioned ?commissioned }
}
LIMIT 50
"""

query = SPARQL_CONFIGS['queries']['get_operational']

rslt = run_query(query=query)
