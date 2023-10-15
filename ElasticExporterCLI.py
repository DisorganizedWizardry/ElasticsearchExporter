"""
Download an elasticsearch index to ndjson using a PIT search

Usage:
  ElasticExportCLI.py --index=<indexname> [--multiple-indexes] [--backup-folder=<backup_folder>] [--query-file=<query_file>] [--export-csv]

Options:
  --index=<indexname>  Set the index to export
  --multiple-indexes   Export multiple indexes at once. use a wildcard for --index=
                       e.g. --index=logstash*
  --backup-folder=<backup_folder>
                       Sets the folder to save the export to
  --query-file=<query_file>
                       Sets a query filter to limit what is exported 
  --export-csv         Also convert the json file to csv.
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
    
  if options.get('--export-csv'):
    settings['export-csv'] = True
  else:
    settings['export-csv'] = False

  if settings.get('debug'):
    print (settings)
    
  if options.get('--multiple-indexes'):
    ElasticExporter.ProcessMultipleIndexes(settings)
    return

  if '*' in settings['index_name']:
    print ("Found wildcard in index name.")
    print ("Use --multiple-indexes to export multiple indexes")
    return
  
  ElasticExporter.ProcessIndex(settings)


if __name__ == "__main__":
  main()


