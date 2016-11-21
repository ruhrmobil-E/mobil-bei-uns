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

from ..models import *
from webapp import mongo, app
import requests
import datetime
import json
import time
from dateutil.parser import parse
from lxml import etree
import sqlalchemy

type_deref = {
  'arbeitsstellen_laengerer_dauer_BadenWuerttemberg': 1,
  'arbeitsstellen_laengerer_dauer_Bayern': 1,
  'arbeitsstellen_laengerer_dauer_Brandenburg': 1,
  'arbeitsstellen_laengerer_dauer_Bremen': 1,
  'arbeitsstellen_laengerer_dauer_Hamburg': 1,
  'arbeitsstellen_laengerer_dauer_Hessen': 1,
  'arbeitsstellen_laengerer_dauer_MecklenburgVorpommern': 1,
  'arbeitsstellen_laengerer_dauer_NordrheinWestfalen': 1,
  'arbeitsstellen_laengerer_dauer_Saarland': 1,
  'arbeitsstellen_laengerer_dauer_Sachsen': 1,
  'arbeitsstellen_laengerer_dauer_SachsenAnhalt': 1,
  'arbeitsstellen_laengerer_dauer_SchleswigHolstein': 1,
  'arbeitsstellen_laengerer_dauer_Thueringen': 1,
  'geschwindigkeitsdaten_NRW': 4,
  'parkdaten_Duesseldorf': 2,
  'parkdaten_Frankfurt': 2,
  'parkdaten_Kassel': 2,
  'sperrhaenger_Hessen': 5,
  'verkehrsmeldungen_Duesseldorf': 6,
  'verkehrsmeldungen_Frankfurt': 6,
  'verkehrsmeldungen_Kassel': 6,
  'verkehrsmeldungen_NRW': 6
}

zoom_level_min = {
  1: 1,
  2: 10,
  3: 13,
  4: 13,
  5: 13,
  6: 10
}

item_meta_keys = {
  ### baustelle
  1: {
    'roadMaintenanceType': 'string', # roadworks, resurfacingWork, roadMarkingWork, repairWork, other, maintenanceWork 	
    'generalPublicComment': 'string',
    'situationRecordCreationTime': 'datetime',
    'laneClosures': 'string',
    'validityStartOfPeriod': 'datetime',
    'validityEndOfPeriod': 'datetime',
    'validityOverallEndTime': 'datetime',
    'validityOverallStartTime': 'datetime',
    'affectedLanes': 'string',
    'affectedCarriagewayLanesLength': 'float',
    'probabilityOfOccurrence': 'string',
    'informationStatus': 'string',
    'constructionWorkType': 'string',
    'constructionWork': 'string',
    'confidentiality': 'string',
    'temporarySpeedLimit': 'float',
    'internalRoadworksIdentifier': 'string',
    'alertCDirectionCoded': 'string',
    'subjectTypeOfWorks': 'string',
    'roadNumber': 'string',
    'id': 'string',
    'situationid': 'string',
    'alertC4LocationTableVersion': 'string',
    'alertC2PrimaryPointLocation': 'integer',
    'alertC2SecondaryPointLocation': 'integer',
    'alertC4SecondaryPointLocation': 'integer',
    'alertC4LocationTableNumber': 'string',
    'alertC4PrimaryPointLocation': 'integer',
    'alertC4LocationCountryCode': 'ignore',
    
    'current': 'string',
    'version': 'string',
    'situationversion': 'string',
    'validityvalidityStatus': 'string',
    'impactExtendedtrackCrossSection': 'string',
    'type': 'string',
    'affectedCarriageway': 'string',
    'impacttrafficConstrictionType': 'string',
    'generalPublicCommentLang': 'ignore',
    'impactresidualRoadWidth': 'string'
  },
  ### parken
  2: {
    'parkingFacilityStatus': 'string', # values: open, closed
    'vehicleHeightComparison': 'string', # values: vehicleHeightComparison
    'vacantParkingSpaces': 'integer',
    'recordVersionTime': 'datetime',
    'openingTimesStartOfPeriod': 'string',
    'parkingFacilityName': 'string',
    'parkingFacilityId': 'string',
    'vehicleHeight': 'float',
    'assignedPersonTypes': 'string', #values (comma separated): disabled, women
    'ownerContactName': 'string',
    'totalParkingCapacity': 'integer', # totalParkingCapacityShortTerm / capacityOverride / capacityShortTermOverride
    'parkingFacilityStatusTime': 'string',
    'parkingFacilityReferenceId': 'string',
    'parkingFacilityDescription': 'string',
    'referenceTargetClass': 'string',
    'openingTimesEndOfPeriod': 'string',
    'parkingFacilityOccupancy': 'ignore',
    'parkingFacilityReference': 'string',
    'parkingFacilityReferenceVersion': 'string',
    'occupiedParkingSpaces': 'integer',
    'capacityLongTermOverride': 'integer',
    'parkingFacilityOccupancyTrend': 'string', #values: decreasing
    'externalLocationCode': 'ignore',
    'externalReferencingSystem': 'ignore',
    'totalParkingCapacityShortTerm': 'ignore',
    'capacityOverride': 'ignore',
    'capacityShortTermOverride': 'ignore',
    'totalParkingCapacityLongTerm': 'ignore', # = totalParkingCapacity
    'parkingFacilityVersion': 'ignore'
  },
  ### geschwindigkeitskontrollen
  4: {
    'anyVehicleAverageVehicleSpeed': 'float',
    'carAverageVehicleSpeed': 'float',
    'lorryAverageVehicleSpeed': 'float',
    'anyVehiclePercentageLongVehicle': 'float',
    'anyVehicleFlowRate': 'integer',
    'carFlowRate': 'integer',
    'lorryFlowRate': 'integer',
    'publicationTime': 'datetime',
    'measurementOrCalculationTime': 'datetime',
    'id': 'string'
  },
  ### sperranhänger
  5: {
    'validityStartOfPeriod': 'datetime',
    'validityEndOfPeriod': 'datetime',
    'validityOverallEndTime': 'datetime',
    'validityOverallStartTime': 'datetime',
    'situationRecordCreationTime': 'datetime',
    'probabilityOfOccurrence': 'string',
    'affectedCarriageway': 'string',
    'complianceOption': 'string',
    'generalPublicComment': 'string',
    'managementType': 'string',
    'roadMaintenanceType': 'string',
    'id': 'string',
    'alertcLocationCountryCode': 'string',
    'alertcLocationTableNumber': 'string',
    'validityStatus': 'string',
    'sourceIdentification': 'string',
    'generalPublicCommentLang': 'string',
    'alertcSpecificlocation': 'string',
    'alertcOffsetDistance': 'string',
    'affectedLanes': 'string',
    'sourceName': 'string',
    'sourceNameLang': 'string',
    'validityPeriodName': 'string',
    'alertcDirectionCoded': 'string',
    'alertcPointType': 'string',
    'alertcDirectionCoded': 'string',
    'alertcLocationTableVersion': 'string',
    'version': 'string',
    'type': 'string'
  },
  6: {
    'tmcEventEventCode': 'string',
    'situationRecordCreationTime': 'datetime',
    'informationStatus': 'string',
    'confidentiality': 'string',
    'validityOverallStartTime': 'datetime',
    'validityOverallEndTime': 'datetime',
    'situationVersion': 'string',
    'validityOverrunning': 'string',
    'version': 'string',
    'probabilityOfOccurrence': 'string',
    'abnormalTrafficType': 'string',
    'alertC4SecondaryPointOffset': 'string',
    'alertC4LocationTableNumber': 'string',
    'alertC4LocationTableVersion': 'string',
    'alertC4PrimaryPointOffset': 'string',
    'alertC4PrimaryPointLocation': 'string',
    'alertC4LocationCountryCode': 'string',
    'alertC4SecondaryPointLocation': 'string',
    'impactCapacityRemaining': 'string',
    'situationVersionTime': 'datetime',
    'situationId': 'string',
    'type': 'string',
    'generalPublicComment': 'string',
    'roadName': 'string',
    'roadManagementType': 'sting',
    'impact_trafficConstrictionType': 'string',
    'validityStatus': 'string',
    'affectedCarriageway': 'string',
    'impactNumberOfLanesRestricted': 'string',
    'id': 'string',
    'roadNumber': 'string',
    'complianceOption': 'string',
    'constructionWorkType': 'string',
    'affectedLanes': 'string',
    'publicEventType': 'string'
  }
}


def sync():
  print "Import MDM GeoJSON"
  base_url = app.config['MDM_S3_STORAGE']
  datafiles_url = base_url
  
  urls_to_sync = []
  last_key = ''
  r = requests.get(datafiles_url, verify=False)
  datafiles = etree.fromstring(r.text.encode('utf-8'), parser=etree.XMLParser(recover=True))
  run_again = True
  while run_again:
    for datafile in datafiles.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
      last_key = datafile.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text
      if '/' not in last_key:
        urls_to_sync.append(last_key)
    if datafiles.find('{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated').text == 'true':
      r = requests.get("%s?marker=%s" % (datafiles_url, last_key), verify=False)
      datafiles = etree.fromstring(r.text.encode('utf-8'), parser=etree.XMLParser(recover=True))
    else:
      run_again = False
  for url_to_sync in urls_to_sync:
    sync_single_url(base_url + url_to_sync)
  
  
def sync_single_url(url):
  print"- process %s" % url
  r = requests.get(url, verify=False)
  try:
    data = r.json()
  except ValueError:
    print "- FATAL: no valid JSON"
    return
  
  traffic_item_provider = mongo.db.traffic_item_provider.find_one({ 'properties.external_id': url })
  if traffic_item_provider:
    traffic_item_provider = traffic_item_provider['_id']
  else:
    traffic_item_provider = mongo.db.traffic_item_provider.insert_one({
      'created': datetime.datetime.now(),
      'external_id': url,
      'active': 1,
      'name': url.split('/')[1].split('.')[0]
    }).inserted_id
  
  
  for raw_traffic_item in data['features']:
    
    # first: get unique id. this depends on data type
    if type_deref[data['name']] == 2:
      external_id = raw_traffic_item['properties']['parkingFacilityId']
    else:
      external_id = raw_traffic_item['properties']['id']
    
    for item_property in raw_traffic_item['properties']:
      if item_property in item_meta_keys[type_deref[data['name']]]:
        if item_meta_keys[type_deref[data['name']]][item_property] == 'datetime':
          if '-' in raw_traffic_item['properties'][item_property]:
            raw_traffic_item['properties'][item_property] = parse(raw_traffic_item['properties'][item_property])
          else:
            raw_traffic_item['properties'][item_property] = datetime.datetime.strptime(raw_traffic_item['properties'][item_property], "%Y%m%d%H%M%S")
          raw_traffic_item['properties'][item_property] = int(raw_traffic_item['properties'][item_property].strftime("%s"))
        if item_meta_keys[type_deref[data['name']]][item_property] == 'float':
          raw_traffic_item['properties'][item_property] = float(raw_traffic_item['properties'][item_property])
        if item_meta_keys[type_deref[data['name']]][item_property] == 'integer':
          raw_traffic_item['properties'][item_property] = int(raw_traffic_item['properties'][item_property])
      else:
        print "new item meta key: %s" % item_property
    raw_traffic_item['properties']['traffic_item_provider'] = traffic_item_provider
    raw_traffic_item['properties']['traffic_item_type'] = type_deref[data['name']]
    raw_traffic_item['properties']['zoom_level_min'] = zoom_level_min[raw_traffic_item['properties']['traffic_item_type']]
    if 'occupiedParkingSpaces' in raw_traffic_item['properties'] and 'totalParkingCapacity' in raw_traffic_item['properties']:
      raw_traffic_item['properties']['occupancy_rate'] = float(raw_traffic_item['properties']['occupiedParkingSpaces']) / float(raw_traffic_item['properties']['totalParkingCapacity'])
    raw_traffic_item['properties']['external_id'] = external_id
    if raw_traffic_item['properties']['traffic_item_type'] == 6:
      raw_traffic_item['properties']['traffic_item_type'] = 1
    
    mongo.db.traffic_item.update({
      'properties.external_id': external_id,
      'properties.traffic_item_provider': traffic_item_provider
    }, {
      '$set': raw_traffic_item
    },
    upsert=True)
    """
    traffic_item = TrafficItem.query.filter_by(external_id=external_id).filter_by(traffic_item_provider_id=traffic_item_provider.id)
    if traffic_item.count():
      traffic_item = traffic_item.first()
    else:
      traffic_item = TrafficItem()
      traffic_item.created = datetime.datetime.now()
      traffic_item.external_id = external_id
      traffic_item.traffic_item_provider_id = traffic_item_provider.id
    traffic_item.active = 1
    traffic_item.updated = datetime.datetime.now()
    if raw_traffic_item['geometry']['type'] == 'Point':
      traffic_item.lat = raw_traffic_item['geometry']['coordinates'][1]
      traffic_item.lon = raw_traffic_item['geometry']['coordinates'][0]
    elif raw_traffic_item['geometry']['type'] == 'LineString':
      traffic_item.lat = raw_traffic_item['geometry']['coordinates'][0][1]
      traffic_item.lon = raw_traffic_item['geometry']['coordinates'][0][0]
    else:
      print "- ignore traffic without location: %s" % external_id
    
    traffic_item.area = json.dumps(raw_traffic_item['geometry'])
    traffic_item.item_type = type_deref[data['name']]
    
    db.session.add(traffic_item)
    db.session.commit()
    
    meta_list = []
    for meta_key, meta_value_type in item_meta_keys[traffic_item.item_type].iteritems():
      if meta_key in raw_traffic_item['properties']:
        value = raw_traffic_item['properties'][meta_key]
        if meta_value_type == 'datetime':
          if '-' in value:
            value = parse(value)
          else:
            value = datetime.datetime.strptime(value, "%Y%m%d%H%M%S")
          value = value.strftime("%s")
        if meta_value_type == 'float':
          value = float(value)
        if meta_value_type == 'integer':
          value = int(value)
        if meta_value_type != 'ignore' and value:
          meta_value = TrafficItemMeta.query.filter_by(traffic_item_id=traffic_item.id).filter_by(key=meta_key)
          if meta_value.count():
            meta_value = meta_value.first()
          else:
            meta_value = TrafficItemMeta()
            meta_value.key = meta_key
            meta_value.traffic_item_id = traffic_item.id
            meta_value.created = datetime.datetime.now()
          meta_value.updated = datetime.datetime.now()
          meta_value.value = value
          
          db.session.add(meta_value)
          db.session.commit()
          
          meta_list.append(meta_key)
        del raw_traffic_item['properties'][meta_key]
    for item in raw_traffic_item['properties']:
      print "- new item found: %s" % json.dumps(item)
    to_delete_items = TrafficItemMeta.query.filter(sqlalchemy.sql.expression.not_(TrafficItemMeta.key.in_(meta_list))).filter_by(traffic_item_id=traffic_item.id).all()
    for to_delete_item in to_delete_items:
      db.session.delete(to_delete_item)
      db.session.commit()
  """
    