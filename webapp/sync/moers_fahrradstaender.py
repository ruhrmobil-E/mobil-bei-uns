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

import requests
import datetime
import json
import bson
from webapp import mongo

def sync():
  print u"Import Moers Fahrradständer"
  url = 'https://www.offenesdatenportal.de/dataset/a7c2b36e-6719-47ef-8845-758928fc5b30/resource/46686088-b524-4552-a43e-5ad535988339/download/fahrradstander.geojson'
  traffic_item_provider = mongo.db.traffic_item_provider.find_one({ 'external_id': url })
  if traffic_item_provider:
    traffic_item_provider = traffic_item_provider['_id']
  else:
    traffic_item_provider = str(mongo.db.traffic_item_provider.insert_one({
      'created': datetime.datetime.now(),
      'external_id': url,
      'active': 1,
      'name': url.split('/')[1].split('.')[0]
    }).inserted_id)
  
  r = requests.get(url, verify=False)
  data = json.loads(r.text.encode('utf-8'))
  
  for raw_traffic_item in data['features']:
    external_id = str(raw_traffic_item['geometry']['coordinates'][0][1]) + '-' + str(raw_traffic_item['geometry']['coordinates'][0][0])
    
    raw_traffic_item['properties']['traffic_item_type'] = 3
    raw_traffic_item['properties']['external_id'] = external_id
    raw_traffic_item['properties']['traffic_item_provider'] = bson.objectid.ObjectId(traffic_item_provider)
    raw_traffic_item['properties']['processed'] = 0
    raw_traffic_item['properties']['zoom_level_min'] = 14
    
    mongo.db.traffic_item.update({
      'properties.external_id': external_id,
      'properties.traffic_item_provider': bson.objectid.ObjectId(traffic_item_provider),
      'properties.processed': 0
    }, {
      '$set': raw_traffic_item
    },
    upsert=True)
    
    raw_traffic_item['properties']['processed'] = 1
    
    mongo.db.traffic_item.update({
      'properties.external_id': external_id,
      'properties.traffic_item_provider': bson.objectid.ObjectId(traffic_item_provider),
      'properties.processed': 1
    }, {
      '$set': raw_traffic_item
    },
    upsert=True)