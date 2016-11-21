# encoding: utf-8

"""
Copyright (c) 2016, Ernesto Ruge
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand

from webapp import app, db, util

manager = Manager(app)
migrate = Migrate(app, db)

manager.add_command('db', MigrateCommand)

@manager.command
def import_regions_to_sql():
  util.import_regions_to_sql()
  
@manager.command
def import_regions_to_es():
  util.import_regions_to_es()

@manager.command
def sync_sources():
  util.sync_sources()

@manager.command
def sync_source(name):
  util.sync_source(name)
  
@manager.command
def import_traffic_items_to_es():
  util.import_traffic_items_to_es()
 
@manager.command
def import_traffic_items_to_es():
  util.import_traffic_items_to_es()

@manager.command
def mongodb_init():
  util.mongodb_init()
if __name__ == "__main__":
  manager.run()