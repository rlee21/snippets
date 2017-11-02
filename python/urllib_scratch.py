import json
import urllib
from urlparse import urlparse, parse_qs

url = 'https://api.typeform.com/v1/form/bNKLG2?key=5e73c9ec7a381f4a2c4e362ff9f5ec8eacc46810&completed=false'
# request = urllib.urlopen(url).read()
# payload = json.loads(request)
# responses = payload['responses']
# for response in responses:
#     print(response)


url2 = 'https://avvoreadyconnect.typeform.com/to/bNKLG2?src=qanda&pid=665496'
parsed = urlparse(url2)
# print 'scheme  :', parsed.scheme
# print 'netloc  :', parsed.netloc
# print 'path    :', parsed.path
# print 'params  :', parsed.params
# print 'query   :', parsed.query
# print 'fragment:', parsed.fragment
# print 'username:', parsed.username
# print 'password:', parsed.password
# print 'hostname:', parsed.hostname, '(netloc in lower case)'
# print 'port    :', parsed.port
# print parse_qs(parsed.query)
tmp = parse_qs(parsed.query)
# print tmp['src'][0]
# print tmp['pid'][0]
print(tmp.get('foo', ''))


# request = urllib.urlopen(url).read()
# payload = json.loads(request)
# responses = payload['responses']
# data = []
# for response in responses:
#     completed = response['completed']
#     metadata = response['metadata']
#     parsed = urlparse(metadata['referer'])
#     print(parsed)