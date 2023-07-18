# ElasticsearchExporter

ElasticsearchExporter can export all events in a single elasticsearch index to a newline-delimited json file. This script uses the elasticsearch python API to perform a PIT and search_after API. It will export 10,000 events at a time and keep going until all events have been exported. It is possible to export millions of events from a single elasticsearch index.

# Download 

> git clone https://github.com/DisorganizedWizardry/ElasticsearchExporter

# Install python requirements

> cd ElasticsearchExporter/
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

