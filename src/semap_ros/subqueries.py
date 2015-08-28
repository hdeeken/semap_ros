#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest('semap_ros')

from sqlalchemy.orm import aliased, join
from sqlalchemy import and_

from db_model import *
from db_environment import db
from semap_ros.srv import *
from semap.ros_postgis_conversion import *

# extend to multiple types via desc.type.in(types)
def any_obj(obj):
  return db().query( obj ) 

def any_obj_type(obj, type):
  desc = aliased( ObjectDescription )
  return db().query( obj ).filter( obj.relative_description_id == desc.id, desc.type == type ) 

def any_obj_ids(obj):
  return db().query( obj.id )

def any_obj_type_ids(obj, type):
  desc = aliased( ObjectDescription )
  return db().query( obj.id ).filter( obj.relative_description_id == desc.id, desc.type == type )

def any_obj_types_ids(obj, types):
  desc = aliased( ObjectDescription )
  return db().query( obj.id ).filter( obj.relative_description_id == desc.id, desc.type.in_(types) )

def obj_geo(obj_id, geo, type):
  return db().query( geo.id ).filter( and_(ObjectInstance.id == obj_id, ObjectInstance.absolute_description_id == geo.abstraction_desc, geo.type == type) )

def get_geometry(obj_id, geo, type):
  return db().query( geo.id ).filter( and_(ObjectInstance.id == obj_id, ObjectInstance.absolute_description_id == geo.geometry_desc, geo.type == type) )

def get_abstraction(obj_id, geo, type):
  return db().query( geo.id ).filter( and_(ObjectInstance.id == obj_id, ObjectInstance.absolute_description_id == geo.abstraction_desc, geo.type == type) )

def obj_geo_ids(obj, geo, type):
  return db().query( geo ).filter( and_(obj.absolute_description_id == geo.abstraction_desc, geo.type == type))
