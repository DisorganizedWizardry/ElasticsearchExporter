# ElasticsearchExporter

ElasticsearchExporter can export all events in a single elasticsearch index to a newline-delimited json file. This script uses the elasticsearch python API to perform a PIT and search_after API. It will export 10,000 events at a time and keep going until all events have been exported. It is possible to export millions of events from a single elasticsearch index.

# Download 

> git clone https://github.com/DisorganizedWizardry/ElasticsearchExporter

# Install python requirements

> cd ElasticsearchExporter/
> 
> pip3 install -r requirements.txt

# Configure local cluster settings 

Configure the elasticsearch python client for your local cluster. All settings will be placed in the configuration file, ElasticExporterSettings.py

If the elasticsearch cluster uses https, then use this command to find the fingerprint and update CERT_FINGERPRINT.
 
> openssl s_client --connect 192.168.1.1:9200 </dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' | openssl x509 -noout -in - --fingerprint -sha256
 
These are the configuration items that need to be updated:

> CERT_FINGERPRINT="00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00"
>
>  es = Elasticsearch(  ['https://192.168.1.1:9200', 'https://192.168.1.2:9200', 'https://192.168.1.3:9200'],
>
>    basic_auth=('username', 'secret'),
>
>    ssl_assert_fingerprint=CERT_FINGERPRINT,
>
>    http_compress=True )

This script uses the elasticsearch python API and the full configuration guide can be found [here](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html)

# Example

This command would then export the index *filebeat-8.8.2* to the folder *exported*

> python3 ElasticExporterCLI.py --index=filebeat-8.8.2 --backup-folder=exported

# Options

```
--index=<indexname>  Set the index to export
--multiple-indexes   Export multiple indexes at once. use a wildcard for --index=
                     e.g. --index=logstash*
--backup-folder=<backup_folder>
                     Sets the folder to save the export to
--query-file=<query_file>
                     Sets a query filter to limit what is exported
--export-csv         Also convert the json file to csv.
```

# Documentation

* [Medium guide - How to Export an Entire elasticsearch Index to a File](https://medium.com/@disorganizedwizardry/how-to-export-an-entire-elasticsearch-index-to-a-file-37667a8803a0)
