# Network architecture

This document describes general principles and architecture for the front end of the TERN-EP eddy covariance data processing pipeline.


## Network nodes

### Hardware

#### Loggers

Most sites use [Campbell Scientific](https://www.campbellsci.com.au/)-based measurement systems; the remainder are [Licor](https://www.licor.com/env/products/eddy-covariance/) SmartFlux systems. We currently consider Campbell systems the most versatile and extensible solution.

#### Instrumentation

Blah blah.

#### Modems

A range of modem types are used (Maxon, Teltonika and Sierra); all have firmware that supports openVPN client, since this is the means of enabling incoming connections to site. 

### Programming

Datalogger programs have historically differed from site to site, reflecting the fact that the network has gradually coalesced from sites that were originally funded as isolated research projects. As such  there are no network-wide variable conventions - variable names and units and data table names differ for the same quantity from site to site. 

While this is undesirable, many site operators have built data processing pipelines around their own conventions, and it is thus difficult to impose new conventions in their stead.

We suggest instead that for future entrants joining the network, we provide a datalogger program that uses network-standard variable conventions. These are similar to, but not the same as, the CF-compliant standards used in the compiled netcdf files (see below xxx); operational variable naming is adopted where divergence from the standard conventions improves clarity for site operators. 

To minimise network complexity, it is crucial to assure that this is the same at all sites.

For sites that already have established variable names embedded in data processing pipelines, we already have the capacity to map and translate the site-specific variable names to network standards, and can thus manage incoming data streams for these sites. However, this increases the complexity (and thus fragility) of the requisite processing pipeline, and increases the amount of information that must be centrally held to manage these data streams. As a solution, in future we aim to roll out modifications to existing datalogger programs at those sites. These modifications write an additional TERN EP-specific data table to memory that will contain data with the correct conventions.

## Central node

Current state:
> Collection and processing of data occurs on a Windows virtual machine (VM) supplied by James Cook University. Data for all sites are collected and stored on a defined schedule. 'Fast' data are written to daily files on the logger  (half-hourly for 'slow' data, daily for ). 
* 
* The current plan is to move to a Linux VM for almost all of this functionality. This is complicated by the fact that the logger communications software requires a Windows OS. However, Campbell offers a Linux-based headless variant of Loggernet, which will be installed on the Linux VM. This requires a Windows machine for interaction and configuration, which will be a VM supplied by University of Melbourne. In the longer term 



A number of configuration files (in .yml format) are required. They contain:
* site-based hardware configurations, including:
    - modem information: type, login, serial number, SIM details, vpnip;
    - logger information: type, serial number, MAC address, vpnip, tables;
    - cameras: in progress.
* global and dimension (time, latitude, longitude) attributes that are (generally) common across all files;
* site-specific variable attributes, including:
    - variable name at site;
    - measurement units;
    - instrument type;
    - deployment height / depth;
    - statistic type;
    - logger table, and;
    - logger name. 

Some notes on this. First, these configuration files will generally be the underlying data source for the project, but how they are generated may change over time. For example, a web-based UI could be used to generate the configuration files. This would solve the problem of how to collect and update site information from the PIs, and would provide an incentive for ensuring that the site information was up to date, since automated processing would otherwise not work.

Second, as a general principle, it is considered desirable to minimise the information held in  configuration files, and maximise the information held in the TERN DSA sites database. Some global attributes are already held in and retrieved from the TERN DSA sites database. A request has been made to include canopy height, tower height, soil description and vegetation description. Moreover, it is planned that the database will also act as an instrument inventory for the network. This will mean that 

Third, it is in principle possible to retrieve measurement units and statistic type either from the file headers, or even from the loggers themselves using the logger [API](https://www.campbellsci.com.au/web-api-article)

The traditional PyFluxPro control files are no longer used - or at least the configuration files effectively replace them. 


