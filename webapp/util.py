# encoding: utf-8

"""
Copyright (c) 2012 - 2016, Ernesto Ruge
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from models import *
from webapp import app, db, es, mongo
import datetime
import calendar
import email.utils
import re
import urllib
import urllib2
import json
import unicodecsv
import translitcodec
import requests
import csv
import traceback
import sqlalchemy
import pymongo
from itertools import cycle
from lxml import etree

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

URL_REGEX = re.compile(
  u"^"
  # protocol identifier
  u"(?:(?:https?|ftp)://)"
  # user:pass authentication
  u"(?:\S+(?::\S*)?@)?"
  u"(?:"
  # IP address exclusion
  # private & local networks
  u"(?!(?:10|127)(?:\.\d{1,3}){3})"
  u"(?!(?:169\.254|192\.168)(?:\.\d{1,3}){2})"
  u"(?!172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2})"
  # IP address dotted notation octets
  # excludes loopback network 0.0.0.0
  # excludes reserved space >= 224.0.0.0
  # excludes network & broadcast addresses
  # (first & last IP address of each class)
  u"(?:[1-9]\d?|1\d\d|2[01]\d|22[0-3])"
  u"(?:\.(?:1?\d{1,2}|2[0-4]\d|25[0-5])){2}"
  u"(?:\.(?:[1-9]\d?|1\d\d|2[0-4]\d|25[0-4]))"
  u"|"
  # host name
  u"(?:(?:[a-z\u00a1-\uffff0-9]-?)*[a-z\u00a1-\uffff0-9]+)"
  # domain name
  u"(?:\.(?:[a-z\u00a1-\uffff0-9]-?)*[a-z\u00a1-\uffff0-9]+)*"
  # TLD identifier
  u"(?:\.(?:[a-z\u00a1-\uffff]{2,}))"
  u")"
  # port number
  u"(?::\d{2,5})?"
  # resource path
  u"(?:/\S*)?"
  u"$"
  , re.UNICODE)

slugify_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.]+')

def sync_sources():
  import sync
  for sync_module in sync.__all__:
    current = getattr(sync, sync_module)
    try:
      current.sync()
    except:
      print "critical error at %s" % sync_module
      traceback.print_exc()

def sync_source(name):
  import sync
  if hasattr(sync, name):
    current = getattr(sync, name)
    current.sync()
  else:
    print "no such source: %s" % name

def mongodb_init():
  mongo.db.traffic_item.find()
  mongo.db.traffic_item.create_index([('location.properties.external_id', pymongo.ASCENDING), ('location.properties.provider_id', pymongo.ASCENDING)])
  mongo.db.traffic_item.create_index([('geometry', '2dsphere')])

def import_regions_to_sql():
  print "Importing %s" % app.config['BUND_REGION_CSV']
  with open(app.config['BUND_REGION_CSV'], 'rb') as region_file:
    region_list = unicodecsv.reader(region_file, delimiter=',', quotechar='"', quoting=unicodecsv.QUOTE_MINIMAL)
    for region_data in region_list:
      if region_data[15] and region_data[16]:
        rgs = region_data[2]
        if region_data[3]:
          rgs += region_data[3]
        else:
          rgs += "0"
        if region_data[4]:
          rgs += region_data[4]
        else:
          rgs += "00"
        if region_data[5]:
          rgs += region_data[5]
        else:
          rgs += "0000"
        if region_data[6]:
          rgs += region_data[6]
        else:
          rgs += "000"
        region = Region.query.filter_by(rgs=rgs)
        if region.count():
          region = region.first()
        else:
          region = Region()
          region.created = datetime.datetime.now()
          region.active = 1
          region.rgs = rgs
        region.lat = region_data[16].replace(',', '.')
        region.lon = region_data[15].replace(',', '.')
        region.updated = datetime.datetime.now()
        
        region.postalcode = region_data[14]
        region.lat = region_data[16].replace(',', '.')
        region.lon = region_data[15].replace(',', '.')
        region.region_level = int(region_data[0])
        
        tmp_name = region_data[7].split(',')
        region.name = tmp_name[0]
        if len(tmp_name) > 1:
          region.name_prefix = tmp_name[0]
        
        region.fulltext = region.postalcode + ' ' + region.name
        region.slug = slugify(region.name + '-' + rgs)
        
        db.session.add(region)
        db.session.commit()


def import_regions_to_es():
  new_index = "%s-%s" % (app.config['REGION_ES'], datetime.datetime.utcnow().strftime('%Y%m%d-%H%M'))
  try:
    es.indices.delete_index(new_index)
  except:
    pass
  
  print "Creating index %s" % new_index
  index_init = {
    'settings': {
      'index': {
        'analysis': {
          'analyzer': {
            'my_simple_german_analyzer': {
              'type': 'custom',
              'tokenizer': 'standard',
              'filter': ['standard', 'lowercase']
            }
          }
        }
      }
    },
    'mappings': {
    }
  }
  index_init['regions'] = {
    'properties': {
      'id': {
        'type': 'string'
      },
      'name': {
        'type': 'string',
        'index': 'analyzed',
        'analyzer': 'my_simple_german_analyzer'
      },
      'slug': {
        'type': 'string'
      },
      'rgs': {
        'type': 'string'
      },
      'postalcode': {
        'type': 'string'
      },
      'location': {
        'type': 'geo_point',
        'lat_lon': True
      },
      'fulltext': {
        'type': 'string',
        'index': 'analyzed',
        'analyzer': 'my_simple_german_analyzer'
      }
    }
  }
  es.indices.create(index=new_index, ignore=400, body=index_init)
  regions = Region.query.all()
  for region in regions:
    dataset = {
      'id': region.id,
      'name': region.name,
      'slug': region.slug,
      'postalcode': region.postalcode,
      'rgs': region.rgs,
      'fulltext': region.fulltext,
      'location': {
        'lat': region.lat,
        'lon': region.lon
      }
    }
    es.index(index=new_index,
             doc_type='regions',
             body=dataset)
  latest_name = '%s-latest' % app.config['REGION_ES']
  alias_update = []
  try:
    latest_before = es.indices.get_alias(latest_name)
    for single_before in latest_before:
      alias_update.append({
        'remove': {
          'index': single_before,
          'alias': latest_name
        }
      })
  except:
    pass
  alias_update.append({
    'add': {
      'index': new_index,
      'alias': latest_name
    }
  })
  print "Aliasing index %s to '%s'" % (new_index, latest_name)
  es.indices.update_aliases({ 'actions': alias_update })
  index_before = es.indices.get('%s*' % app.config['REGION_ES'])
  for single_index in index_before:
    if new_index != single_index:
      print "Deleting index %s" % single_index
      es.indices.delete(single_index)
  

def import_traffic_items_to_es():
  new_index = "%s-%s" % (app.config['TRAFFIC_ITEMS_ES'], datetime.datetime.utcnow().strftime('%Y%m%d-%H%M'))
  try:
    es.indices.delete_index(new_index)
  except:
    pass
  
  print "Creating index %s" % new_index
  settings = {
    'index': {
      'analysis': {
        'analyzer': {
          'my_simple_german_analyzer': {
            'type': 'custom',
            'tokenizer': 'standard',
            'filter': ['standard', 'lowercase']
          },
          'sort_analyzer': {
            'tokenizer': 'keyword',
            'filter': ['lowercase', 'asciifolding']
          }
        }
      }
    }
  }
  mappings = {
    'properties': {
      'id': {
        'type': 'string'
      },
      'location': {
        'type': 'geo_point',
        'lat_lon': True
      },
      'area_points': {
        'type': 'geo_point',
        'lat_lon': True
      },
      'item_type': {
        'type': 'integer'
      },
      'start': {
        'type': 'integer'
      },
      'end': {
        'type': 'integer'
      },
      'occupancy': {
        'type': 'float'
      },
      'traffic_item_provider': {
        'properties': {
          'id': {
            'type': 'integer'
          },
          'name': {
            'type': 'string'
          }
        }
      }
    }
  }
  es.indices.create(index=new_index, ignore=400, body={
    'settings': settings,
    'mappings': {
      'traffic_item': mappings
    }
  })
  traffic_items = TrafficItem.query.filter(sqlalchemy.not_(TrafficItem.item_type == 4)).all()
  for traffic_item in traffic_items:
    traffic_item_provider = TrafficItemProvider.query.filter_by(id=traffic_item.traffic_item_provider_id).first()
    dataset = {
      'id': traffic_item.id,
      'location': {
        'lat': traffic_item.lat,
        'lon': traffic_item.lon
      },
      'traffic_item_type': traffic_item.item_type,
      'area_points': [],
      'traffic_item_provider': {
        'id': traffic_item_provider.id,
        'name': traffic_item_provider.name
      }
    }
    # Preparation of search based on map viewport
    if traffic_item.area:
      area = json.loads(traffic_item.area)
      if area['type'] == 'Point':
        dataset['area_points'].append({
            'lat': area['coordinates'][1],
            'lon': area['coordinates'][0]
        })
      elif area['type'] == 'LineString':
        for point in area['coordinates']:
          dataset['area_points'].append({
            'lat': point[1],
            'lon': point[0]
          })
    
      for i in range(0, len(area['coordinates'])):
        area['coordinates'][i] = [area['coordinates'][i][1], area['coordinates'][i][0]]
      dataset['area'] = json.dumps(area)
    
    # baustelle
    if traffic_item.item_type == 1:
      # start
      traffic_item_meta = TrafficItemMeta.query.filter_by(traffic_item_id=traffic_item.id).filter_by(key='validityOverallStartTime')
      if traffic_item_meta.count():
        dataset['start'] = traffic_item_meta.first().value
      # end
      traffic_item_meta = TrafficItemMeta.query.filter_by(traffic_item_id=traffic_item.id).filter_by(key='validityOverallEndTime')
      if traffic_item_meta.count():
        dataset['end'] = traffic_item_meta.first().value
    # parkplatz
    elif traffic_item.item_type == 2:
      # occupied / total = occupancy_rate
      traffic_item_meta_occupied = TrafficItemMeta.query.filter_by(traffic_item_id=traffic_item.id).filter_by(key='occupiedParkingSpaces')
      traffic_item_meta_total = TrafficItemMeta.query.filter_by(traffic_item_id=traffic_item.id).filter_by(key='totalParkingCapacity')
      if traffic_item_meta_occupied.count() and traffic_item_meta_total.count():
        dataset['occupancy_rate'] = int(traffic_item_meta_occupied.first().value) / int(traffic_item_meta_total.first().value)
    
    es.index(index=new_index,
             doc_type='traffic_item',
             body=dataset)
  latest_name = '%s-latest' % app.config['TRAFFIC_ITEMS_ES']
  alias_update = []
  try:
    latest_before = es.indices.get_alias(latest_name)
    for single_before in latest_before:
      alias_update.append({
        'remove': {
          'index': single_before,
          'alias': latest_name
        }
      })
  except:
    pass
  alias_update.append({
    'add': {
      'index': new_index,
      'alias': latest_name
    }
  })
  print "Aliasing index %s to '%s'" % (new_index, latest_name)
  es.indices.update_aliases({ 'actions': alias_update })
  index_before = es.indices.get('%s*' % app.config['TRAFFIC_ITEMS_ES'])
  for single_index in index_before:
    if new_index != single_index:
      print "Deleting index %s" % single_index
      es.indices.delete(single_index)

# Creates a slug
def slugify(text, delim=u'-'):
  """Generates an ASCII-only slug."""
  result = []
  for word in slugify_re.split(text.lower()):
    word = word.encode('translit/long')
    if word:
      result.append(word)
  return unicode(delim.join(result))

def rfc1123date(value):
  """
  Gibt ein Datum (datetime) im HTTP Head-tauglichen Format (RFC 1123) zurück
  """
  tpl = value.timetuple()
  stamp = calendar.timegm(tpl)
  return email.utils.formatdate(timeval=stamp, localtime=False, usegmt=True)

def expires_date(hours):
  """Date commonly used for Expires response header"""
  dt = datetime.datetime.now() + datetime.timedelta(hours=hours)
  return rfc1123date(dt)

def cache_max_age(hours):
  """String commonly used for Cache-Control response headers"""
  seconds = hours * 60 * 60
  return 'max-age=' + str(seconds)

def obscuremail(mailaddress):
  return mailaddress.replace('@', '__AT__').replace('.', '__DOT__')
app.jinja_env.filters['obscuremail'] = obscuremail


def obscuremail(mailaddress):
  return mailaddress.replace('@', '__AT__').replace('.', '__DOT__')
app.jinja_env.filters['obscuremail'] = obscuremail

def numberformat(number):
  cyc = cycle(['', '', '.'])
  s = str(number)
  last = len(s) - 1
  formatted = [(cyc.next() if idx != last else '') + char
    for idx, char in enumerate(reversed(s))]
  return ''.join(reversed(formatted))
app.jinja_env.filters['numberformat'] = numberformat

class MyEncoder(json.JSONEncoder):
  def default(self, obj):
    return str(obj)
