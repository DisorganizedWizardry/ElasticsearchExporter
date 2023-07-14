"""
Download an elasticsearch index to ndjson using a PIT search

Usage:
  ElasticExportCLI.py --index=<indexname> --backup-folder=<backup_folder> [--query-filter=<query-filter>] [--no_group]
"""

from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import os, sys, json
import traceback
from docopt import docopt

#library for ElasticExporter
import ElasticExporter

#local config 
import ElasticExporterSettings

def main():
  #Load local config
  settings = ElasticExporterSettings.LoadSettings()

  if settings['debug']:
    print ("Loaded settings : %s" % settings)


  options = docopt(__doc__)

  if options['--index']:
    settings['index_name'] = options['--index']

  if options['--no_group']:
    settings['NoGroup'] = True
  else:
    settings['NoGroup'] = False

  if options['--query-filter']:
    with open (options['--query-filter'], 'rb') as f:
      settings['query_filter'] = json.load(f)
    if settings['debug']:
      print ("Loaded Filter : %s" % settings['query_filter'])
  else:
    #Default filter match_all
    settings['query_filter'] = { "bool": { "filter": [ { "match_all": {} } ], } }

  #folder to save exported ndjson files
  if options['--backup-folder']:
    settings['backup_folder'] = options['--backup-folder']


  if settings['debug']:
    print (settings)

  ElasticExporter.ProcessIndex(settings)


if __name__ == "__main__":
  main()


