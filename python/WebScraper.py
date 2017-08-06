import urllib

url = 'https://developer.yahoo.com/'
data = urllib.urlopen(url).read()
print(data)