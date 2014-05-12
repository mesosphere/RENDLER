#!/usr/bin/env python
import urlparse, urllib, sys
from bs4 import BeautifulSoup

url = "http://mesosphere.io/"

source = urllib.urlopen(url).read()
soup = BeautifulSoup(source)

try:
  for item in soup.find_all('a'):
      try:
          print urlparse.urljoin(url, item.get('href'))
      except:
          pass # Not a valid link
except:
  print "Could not fetch any links from html"
  sys.exit(1)
