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

from sqlalchemy.ext.declarative import declarative_base
from webapp import db

Base = declarative_base()

class TrafficItemProvider(db.Model):
  __tablename__ = 'traffic_item_provider'
  id = db.Column(db.Integer(), primary_key=True)
  
  created = db.Column(db.DateTime())
  updated = db.Column(db.DateTime())
  active = db.Column(db.Integer())
  
  name = db.Column(db.String(255))
  slug = db.Column(db.String(255))
  descr_short = db.Column(db.Text())
  descr = db.Column(db.Text())
  external_id = db.Column(db.String(255))
  
  email = db.Column(db.String(255))
  website = db.Column(db.String(255))
  licence = db.Column(db.Text())
  
  street = db.Column(db.String(255))
  postalcode = db.Column(db.String(5))
  city = db.Column(db.String(255))
  
  traffic_item = db.relationship("TrafficItem", backref="TrafficItemProvider", lazy='dynamic')
  

class TrafficItem(db.Model):
  __tablename__ = 'traffic_item'
  id = db.Column(db.Integer(), primary_key=True)
  
  created = db.Column(db.DateTime())
  updated = db.Column(db.DateTime())
  active = db.Column(db.Integer())
  
  name = db.Column(db.String(255))
  slug = db.Column(db.String(255))
  external_id = db.Column(db.String(255))
  
  item_type = db.Column(db.Integer()) # 1 Baustelle 2 Auto-Parkplatz 3 Fahrrad-Ständer 4 Verkehrszählung 5 Sperranhänger
  raw_status = db.Column(db.Integer()) # 1 komplett roh, 2 verarbeitet
  is_meta = db.Column(db.Integer())
  
  zoom_from = db.Column(db.Integer())
  zoom_till = db.Column(db.Integer())
  
  lat = db.Column(db.Numeric(precision=10,scale=7))
  lon = db.Column(db.Numeric(precision=10,scale=7))
  
  area = db.Column(db.Text())
  
  traffic_item_provider_id = db.Column(db.Integer, db.ForeignKey('traffic_item_provider.id'))
  traffic_item_meta = db.relationship("TrafficItemMeta", backref="TrafficItem", lazy='dynamic')
  
  def __init__(self):
    pass

  def __repr__(self):
    return '<TrafficItem %r>' % self.name

class TrafficItemMeta(db.Model):
  __tablename__ = 'traffic_item_meta'
  id = db.Column(db.Integer(), primary_key=True)
  
  created = db.Column(db.DateTime())
  updated = db.Column(db.DateTime())
  active = db.Column(db.Integer())
  
  key = db.Column(db.String(255))
  value = db.Column(db.String(255))
  
  traffic_item_id = db.Column(db.Integer, db.ForeignKey('traffic_item.id'))
  
  def __init__(self):
    pass

  def __repr__(self):
    return '<TrafficItemMeta %r>' % self.name


class Region(db.Model):
  __tablename__ = 'region'
  
  id = db.Column(db.Integer(), primary_key=True)
  name = db.Column(db.String(255))
  slug = db.Column(db.String(255), unique=True)
  created = db.Column(db.DateTime())
  updated = db.Column(db.DateTime())
  active = db.Column(db.Integer())
  
  osm_id = db.Column(db.Integer())
  geo_json = db.Column(db.Text())
  rgs = db.Column(db.String(255))
  region_level = db.Column(db.Integer())
  postalcode = db.Column(db.String(255))
  fulltext = db.Column(db.String(255))
  
  lat = db.Column(db.Numeric(precision=10,scale=7))
  lon = db.Column(db.Numeric(precision=10,scale=7))
  
  def __init__(self):
    pass

  def __repr__(self):
    return '<Hoster %r>' % self.name
