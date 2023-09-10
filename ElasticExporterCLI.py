"""
Download an elasticsearch index to ndjson using a PIT search

Usage:
  ElasticExportCLI.py --index=<indexname> --backup-folder=<backup_folder> [--query-file=<query-file>]
"""

from elasticsearch import Elasticsearch
import json
from docopt import docopt

#library for ElasticExporter
import ElasticExporter

#local config 
import ElasticExporterSettings

def main():
  #Load local config
  settings = ElasticExporterSettings.LoadSettings()

  if settings.get('debug'):
    print ("Loaded settings : %s" % settings)


  options = docopt(__doc__)

  if options.get('--index'):
    settings['index_name'] = options['--index']

  #if options['--no_group']:
  #  settings['NoGroup'] = True
  #else:
  #  settings['NoGroup'] = False
  #set default setting until this feature is added
  settings['NoGroup'] = False

  if options.get('--query-file'):
    with open (options['--query-file'], 'rb') as f:
      settings['query_filter'] = json.load(f)
    if settings.get('debug'):
      print ("Loaded Filter : %s" % settings['query_filter'])
  else:
    #Default filter match_all
    settings['query_filter'] = { "bool": { "filter": [ { "match_all": {} } ], } }

  #folder to save exported ndjson files
  if options.get('--backup-folder'):
    settings['backup_folder'] = options['--backup-folder']


  if settings.get('debug'):
    print (settings)

  ElasticExporter.ProcessIndex(settings)


if __name__ == "__main__":
  main()


