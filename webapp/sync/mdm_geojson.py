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

from webapp import mongo, app
import requests
import datetime
import json
import time
import bson
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
  
  for raw_traffic_item in data['features']:
    save_traffic_item(traffic_item_provider, raw_traffic_item, type_deref[data['name']])


def save_traffic_item(traffic_item_provider, raw_traffic_item, traffic_item_type):
  # first: get unique id. this depends on data type
  if traffic_item_type == 2:
    external_id = raw_traffic_item['properties']['parkingFacilityId']
  else:
    external_id = raw_traffic_item['properties']['id']
  
  for item_property in raw_traffic_item['properties']:
    if item_property in item_meta_keys[traffic_item_type]:
      if item_meta_keys[traffic_item_type][item_property] == 'datetime':
        if '-' in raw_traffic_item['properties'][item_property]:
          raw_traffic_item['properties'][item_property] = parse(raw_traffic_item['properties'][item_property])
        else:
          raw_traffic_item['properties'][item_property] = datetime.datetime.strptime(raw_traffic_item['properties'][item_property], "%Y%m%d%H%M%S")
        raw_traffic_item['properties'][item_property] = int(raw_traffic_item['properties'][item_property].strftime("%s"))
      if item_meta_keys[traffic_item_type][item_property] == 'float':
        raw_traffic_item['properties'][item_property] = float(raw_traffic_item['properties'][item_property])
      if item_meta_keys[traffic_item_type][item_property] == 'integer':
        raw_traffic_item['properties'][item_property] = int(raw_traffic_item['properties'][item_property])
    else:
      print "new item meta key: %s" % item_property
  raw_traffic_item['properties']['traffic_item_provider'] = bson.objectid.ObjectId(traffic_item_provider)
  raw_traffic_item['properties']['traffic_item_type'] = traffic_item_type
  raw_traffic_item['properties']['zoom_level_min'] = zoom_level_min[raw_traffic_item['properties']['traffic_item_type']]
  if 'occupiedParkingSpaces' in raw_traffic_item['properties'] and 'totalParkingCapacity' in raw_traffic_item['properties']:
    if raw_traffic_item['properties']['occupiedParkingSpaces'] and raw_traffic_item['properties']['totalParkingCapacity']:
      raw_traffic_item['properties']['occupancy_rate'] = float(raw_traffic_item['properties']['occupiedParkingSpaces']) / float(raw_traffic_item['properties']['totalParkingCapacity'])
  raw_traffic_item['properties']['external_id'] = external_id
  if raw_traffic_item['properties']['traffic_item_type'] == 6:
    raw_traffic_item['properties']['traffic_item_type'] = 1
  raw_traffic_item['properties']['processed'] = 0
  
  mongo.db.traffic_item.update({
    'properties.external_id': external_id,
    'properties.traffic_item_provider': bson.objectid.ObjectId(traffic_item_provider),
    'properties.processed': 0
  }, {
    '$set': raw_traffic_item
  },
  upsert=True)
  generate_processed_traffic_item(traffic_item_provider, external_id, raw_traffic_item)
  
def generate_processed_traffic_item(traffic_item_provider, external_id, traffic_item):
  if 'alertC2PrimaryPointLocation' in traffic_item['properties'] and 'alertC2SecondaryPointLocation' in traffic_item['properties']:
    start_lcl = traffic_item['properties']['alertC2PrimaryPointLocation']
    end_lcl = traffic_item['properties']['alertC2SecondaryPointLocation']
  
    existing_route = mongo.db.lcl_route.find_one({
      'start': start_lcl,
      'end': end_lcl
    })
    if existing_route:
      traffic_item['geometry'] = existing_route['geometry']
  
  traffic_item['properties']['processed'] = 1
  mongo.db.traffic_item.update({
    'properties.external_id': external_id,
    'properties.traffic_item_provider': bson.objectid.ObjectId(traffic_item_provider),
    'properties.processed': 1
  }, {
    '$set': traffic_item
  },
  upsert=True)
  