from elasticsearch import Elasticsearch

def LoadSettings():

  #command to get the SHA-256 fingerprint from the elasticsearch server
  #echo -n | openssl s_client --connect 192.168.1.1:9200 | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' | openssl x509 -noout -in - --fingerprint -sha256

  CERT_FINGERPRINT="00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00"

  es = Elasticsearch(  ['https://192.168.1.1:9200', 'https://192.168.1.2:9200', 'https://192.168.1.3:9200'],
    basic_auth=('username', 'secret'),
    ssl_assert_fingerprint=CERT_FINGERPRINT,
    http_compress=True )

  settings = { 'es' : es }

  settings['TimeSeries'] = True
  settings['timestamp'] = '@timestamp'

  #enable debug logging
  settings['debug'] = False

  return settings

if __name__ == '__main__':
  print ("This is the config and settings for Elasticsearch Exporter")
