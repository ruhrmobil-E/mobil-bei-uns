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

from webapp import app, es, mongo
from flask import render_template, make_response, abort, request, Response, redirect, flash, send_file
from models import *
import util
import json, time, os, datetime
import elasticsearch

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/anbieter')
def anbieter():
  sharing_providers_data = SharingProvider.query.filter_by(active=1).order_by(SharingProvider.name).all()
  sharing_providers = []
  for sharing_provider_data in sharing_providers_data:
    sharing_provider_data.vehicle_station_count = SharingStation.query.filter_by(sharing_provider_id=sharing_provider_data.id).count()
    sharing_providers.append(sharing_provider_data)
  return render_template('anbieter.html', sharing_providers=sharing_providers)

@app.route('/anbieter/<string:anbieter_slug>')
def anbieter_single(anbieter_slug):
  sharing_provider = SharingProvider.query.filter_by(active=1).filter_by(slug=anbieter_slug).first_or_404()
  sharing_provider.vehicle_station_count = SharingStation.query.filter_by(sharing_provider_id=sharing_provider.id).count()
  return render_template('anbieter-single.html', sharing_provider=sharing_provider)

@app.route('/ueber-das-projekt')
def ueber_das_projekt():
  return render_template('ueber-das-projekt.html')

@app.route('/daten-und-api')
def daten_und_api():
  sharing_providers = SharingProvider.query.filter_by(active=1).order_by(SharingProvider.name).all()
  return render_template('daten-und-api.html', sharing_providers=sharing_providers)

@app.route('/impressum')
def impressum():
  return render_template('impressum.html')

@app.route('/datenschutz')
def datenschutz():
  return render_template('datenschutz.html')

@app.route('/api/traffic-item-providers')
def api_providers():
  start_time = time.time()
  provider_id = request.args.get('id', 0, type=int)
  result = []
  providers = SharingProvider.query.filter_by(active=1).filter(SharingProvider.id > provider_id).order_by(SharingProvider.id).limit(100).all()
  for provider in providers:
    result.append({
      'id': provider.id,
      'name': provider.name,
      'website': provider.website,
      'licence': provider.licence
    })
  ret = {
    'status': 0,
    'duration': round((time.time() - start_time) * 1000),
    'data': result,
    'next': '/api/traffic-item-providers?id=%s' % (provider_id + 100)
  }
  json_output = json.dumps(ret, cls=util.MyEncoder, sort_keys=True)
  response = make_response(json_output, 200)
  response.mimetype = 'application/json'
  response.headers['Expires'] = util.expires_date(hours=24)
  response.headers['Cache-Control'] = util.cache_max_age(hours=24)
  return(response)

@app.route('/api/traffic-items')
def api_station():
  start_time = time.time()
  station_id = request.args.get('id', 0, type=int)
  result = []
  stations = SharingStation.query.filter_by(active=1).filter(SharingStation.id > station_id).order_by(SharingStation.id).limit(100).all()
  for station in stations:
    result.append({
      'id': station.id,
      'name': station.name,
      'slug': station.slug,
      'lat': station.lat,
      'lon': station.lon,
      'vehicle_all': station.vehicle_all,
      'provider_id': station.sharing_provider_id
    })
  ret = {
    'status': 0,
    'duration': round((time.time() - start_time) * 1000),
    'data': result,
    'next': '/api/traffic-items?id=%s' % (station_id + 100)
  }
  json_output = json.dumps(ret, cls=util.MyEncoder, sort_keys=True)
  response = make_response(json_output, 200)
  response.mimetype = 'application/json'
  response.headers['Expires'] = util.expires_date(hours=24)
  response.headers['Cache-Control'] = util.cache_max_age(hours=24)
  return(response)


@app.route("/api/traffic-item")
def api_traffic_item():
  start_time = time.time()
  traffic_item_id = request.args.get('id', 0, type=int)
  if not traffic_item_id:
    abort(404)
  traffic_item_data = TrafficItem.query.filter_by(id=traffic_item_id).first_or_404()
  traffic_item = {
    'id': traffic_item_data.id,
    'lat': traffic_item_data.lat,
    'lon': traffic_item_data.lon,
    'zoom_from': traffic_item_data.zoom_from,
    'zoom_till': traffic_item_data.zoom_till,
    'external_id': traffic_item_data.external_id,
    'created': traffic_item_data.created,
    'updated': traffic_item_data.updated,
    'properties': {}
  }
  if traffic_item_data.area:
    traffic_item['area'] = json.loads(traffic_item_data.area)
  traffic_item_metas = TrafficItemMeta.query.filter_by(traffic_item_id=traffic_item_data.id).all()
  for traffic_item_meta in traffic_item_metas:
    traffic_item['properties'][traffic_item_meta.key] = traffic_item_meta.value
  ret = {
    'status': 0,
    'duration': round((time.time() - start_time) * 1000),
    'response': traffic_item
  }
  json_output = json.dumps(ret, cls=util.MyEncoder, sort_keys=True)
  response = make_response(json_output, 200)
  response.mimetype = 'application/json'
  response.headers['Expires'] = util.expires_date(hours=24)
  response.headers['Cache-Control'] = util.cache_max_age(hours=24)
  return(response)

@app.route('/search/traffic-items', methods=['GET', 'POST'])
def search_traffic_items():
  start_time = time.time()
  limits = request.form.get('l', None)
  traffic_item_type = request.form.get('traffic_item_type', None)
  construction_site_date = request.form.get('date', None)
  occupancy_rate = request.form.get('occupancy_rate', None)
  zoom = request.form.get('zoom', None)
  processed = request.form.get('processed', 1, type=int)
  
  saved_request = {
    'limits': limits,
    'traffic_item_type': traffic_item_type,
    'construction_site_date': construction_site_date,
    'occupancy_rate': occupancy_rate,
    'zoom': zoom
  }
  limits = limits.split(';')
  
  result_raw = mongo.db.traffic_item.find({
    '$and': [
      {
        'properties.processed': processed
      },
      {
        'geometry': {
          '$geoIntersects': {
            '$geometry': {
              'type':  'Polygon',
              'coordinates': [
                [
                  [float(limits[2]), float(limits[0])],
                  [float(limits[2]), float(limits[1])],
                  [float(limits[3]), float(limits[1])],
                  [float(limits[3]), float(limits[0])],
                  [float(limits[2]), float(limits[0])]
                ]
              ]
            }
          }
        }
      },
      {
        'properties.zoom_level_min': {
          '$lt': int(zoom)
        }
      },
      {
        '$or': [
          {
            '$and': [
              {
                'properties.validityOverallStartTime': {
                  '$lt': int(construction_site_date)
                },
              },
              {
                'properties.validityOverallEndTime': {
                  '$gt': int(construction_site_date)
                },
              },
              {
                'properties.traffic_item_type': {
                  '$eq': 1
                }
              }
            ]
          },
          {
            '$and': [
              {
                'properties.occupancy_rate': {
                  '$lt': float(occupancy_rate) / 100.0
                }
              },
              {
                'properties.traffic_item_type': {
                  '$eq': 2
                }
              }
            ]
          },
          {
            
            'properties.traffic_item_type': {
              '$eq': 3
            }
          }
        ]
      }
    ]
  })
  result = []
  for value in result_raw:
    result.append(value)
  
  ret = {
    'status': 0,
    'request': saved_request,
    'duration': round((time.time() - start_time) * 1000),
    'response': {
      'type': 'FeatureCollection',
      'features': result
    }
  }
  json_output = json.dumps(ret, cls=util.MyEncoder, sort_keys=True)
  response = make_response(json_output, 200)
  response.mimetype = 'application/json'
  response.headers['Expires'] = util.expires_date(hours=24)
  response.headers['Cache-Control'] = util.cache_max_age(hours=24)
  return(response)


@app.route("/search/traffic-items-es", methods=['GET', 'POST'])
def search_traffic_items_es():
  start_time = time.time()
  limits = request.form.get('l', None)
  traffic_item_type = request.form.get('traffic_item_type', None)
  construction_site_date = request.form.get('date', None)
  occupancy_rate = request.form.get('occupancy_rate', None)
  zoom = request.form.get('zoom', None)
  saved_request = {
    'limits': limits,
    'traffic_item_type': traffic_item_type,
    'construction_site_date': construction_site_date,
    'occupancy_rate': occupancy_rate,
    'zoom': zoom
  }
  if limits:
    limits = limits.split(';')
  
  query_parts_must = []
  query_parts_should = []
  
  if traffic_item_type:
    traffic_item_type = traffic_item_type.split(',')
    query_parts_must.append({
      'terms': {
        'traffic_item_type': traffic_item_type
      }
    })
  
  
  if '1' in traffic_item_type:
    query_parts_should.append(
      {
        'bool': {
          'must': [
            {
              'range': {
                'start': {
                  'lte': construction_site_date
                }
              }
            },
            {
              'range': {
                'end': {
                  'gte': construction_site_date
                }
              }
            },
            {
              'term': {
                'traffic_item_type': 1
              }
            }
          ]
        }
      }
    )
    
  
  if '2' in traffic_item_type:
    query_parts_should.append(
      {
        'bool': {
          'must': [
            #{
            #  'range': {
            #    'occupancy_rate': {
            #      'gte': occupancy_rate
            #    }
            #  }
            #},
            {
              'term': {
                'traffic_item_type': 2
              }
            }
          ]
        }
      }
    )
  if limits:
    limit_queries = {}
    for limit in limits:
      if limit.find('<=') >= 0:
        limit_split = limit.split('<=')
        if (limit_split[0] not in limit_queries):
          limit_queries[limit_split[0]] = {}
        limit_queries[limit_split[0]]['lte'] = limit_split[1]
      elif limit.find('>=') >= 0:
        limit_split = limit.split('>=')
        if (limit_split[0] not in limit_queries):
          limit_queries[limit_split[0]] = {}
        limit_queries[limit_split[0]]['gte'] = limit_split[1]
      elif limit.find('>') >= 0:
        limit_split = limit.split('>')
        if (limit_split[0] not in limit_queries):
          limit_queries[limit_split[0]] = {}
        limit_queries[limit_split[0]]['lt'] = limit_split[1]
      elif limit.find('<') >= 0:
        limit_split = limit.split('<')
        if (limit_split[0] not in limit_queries):
          limit_queries[limit_split[0]] = {}
        limit_queries[limit_split[0]]['lt'] = limit_split[1]
    for limit_query_key, limit_query_value in limit_queries.iteritems():
      query_parts_must.append({
        'range': {
          limit_query_key: limit_query_value
        }
      })
  
  query = {
    'query': {
      'constant_score': {
        'filter': {
          'bool': {
            'must': [{"match_all": {}}] + query_parts_must,
            'should': query_parts_should
          }
        }
      }
    }
  }
  
  es_result = es.search(
    index = app.config['TRAFFIC_ITEMS_ES'] + '-latest',
    doc_type = 'traffic_item',
    fields = 'id,location.lat,location.lon,traffic_item_type,area,start,end,occupancy_rate',
    body = query,
    size = 10000
  )
  result = []
  for single in es_result['hits']['hits']:
    item = {
      'id': single['fields']['id'][0],
      'lat': single['fields']['location.lat'][0],
      'lon': single['fields']['location.lon'][0],
      'type': single['fields']['traffic_item_type'][0]
    }
    if 'area' in single['fields']:
      item['area'] = json.loads(single['fields']['area'][0])
    if 'start' in single['fields']:
      item['start'] = single['fields']['start'][0]
    if 'end' in single['fields']:
      item['end'] = single['fields']['end'][0]
    if 'occupancy_rate' in single['fields']:
      item['occupancy_rate'] = single['fields']['occupancy_rate'][0]
    result.append(item)
  ret = {
    'status': 0,
    'request': saved_request,
    'duration': round((time.time() - start_time) * 1000),
    'response': result
  }
  json_output = json.dumps(ret, cls=util.MyEncoder, sort_keys=True)
  response = make_response(json_output, 200)
  response.mimetype = 'application/json'
  response.headers['Expires'] = util.expires_date(hours=24)
  response.headers['Cache-Control'] = util.cache_max_age(hours=24)
  return(response)



@app.route("/search-region-live")
def search_region_live():
  start_time = time.time()
  result = []
  search_string = request.args.get('q', False)
  # generate fulltext search string
  if not search_string:
    search_results = []
  else:
    search_string = search_string.split()
    search_string_to_complete = search_string[-1]
    query_parts = []
    query_parts.append({
      'match_phrase_prefix': {
        'fulltext': search_string_to_complete.lower()
      }
    })
    if len(search_string[0:-1]):
      query_parts.append({
        'query_string': {
          'fields': ['fulltext'],
          'query': " ".join(search_string[0:-1]),
          'default_operator': 'and'
        }
      })
    try:
      result = es.search(
        index = "%s-latest" % app.config['REGION_ES'],
        doc_type = 'regions',
        fields = 'name,slug,postalcode,location.lat,location.lon',
        body = {
          'query': {
            'bool': {
              'must': query_parts
            }
          },
          'aggs': {
            'fragment': {
              'terms': {
                'field': 'fulltext',
                'include': {
                  'pattern': search_string_to_complete.lower() + '.*',
                  'flags': 'CANON_EQ|CASE_INSENSITIVE',
                },
                'min_doc_count': 0,
                'size': 10
              }
            }
          }
        },
        size = 10
      )
    except elasticsearch.NotFoundError:
      abort(403)
    search_results = []
    for dataset in result['hits']['hits']:
      tmp_search_result = {
        'name': dataset['fields']['name'][0],
        'postalcode': dataset['fields']['postalcode'][0],
        'slug': dataset['fields']['slug'][0],
        'lat': dataset['fields']['location.lat'][0],
        'lon': dataset['fields']['location.lon'][0],
      }
      search_results.append(tmp_search_result)

  ret = {
    'status': 0,
    'duration': round((time.time() - start_time) * 1000),
    'response': search_results
  }
  json_output = json.dumps(ret, cls=util.MyEncoder, sort_keys=True)
  response = make_response(json_output, 200)
  response.mimetype = 'application/json'
  response.headers['Expires'] = util.expires_date(hours=24)
  response.headers['Cache-Control'] = util.cache_max_age(hours=24)
  return(response)
