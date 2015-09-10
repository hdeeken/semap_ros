#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest('semap_ros')

from sqlalchemy.orm import aliased, join
from sqlalchemy import and_, or_

from db_model import *
from db_environment import db
from semap_ros.srv import *
from semap.ros_postgis_conversion import *

## Object Returning

def any_obj(obj):
  return db().query( obj )

def any_obj_type(obj, type):
  desc = aliased( ObjectDescription )
  return db().query( obj ).filter( obj.relative_description_id == desc.id, desc.type == type )

# Object ID Returning

def any_obj_ids(obj):
  return db().query( obj.id )

def any_obj_type_ids(obj, type):
  desc = aliased( ObjectDescription )
  return db().query( obj.id ).filter( obj.relative_description_id == desc.id, desc.type == type )

def any_obj_types_ids(obj, types):
  desc = aliased( ObjectDescription )
  query = db().query( obj.id ).filter( obj.relative_description_id == desc.id, or_(*[desc.type.contains(type) for type in types]) )
  return query

## Descriptions Returning

def desc_all(desc):
  return db().query( desc )

def desc_by_types(desc, types):
  return db().query( desc ).filter( or_(*[desc.type.contains(type) for type in types]) )

# Descriptions ID Returning

def desc_ids_all(desc):
  return db().query( desc.id )

def desc_ids_by_type(desc, types):
  return db().query( desc.id ).filter( or_(*[desc.type.contains(type) for type in types]) )

# Geometry Returning

def obj_geo_ids(obj, geo, type):
  return db().query( geo ).filter( and_(obj.absolute_description_id == geo.abstraction_desc, geo.type == type))

# Geometry ID Returning

def obj_geo(obj_id, geo, type):
  return db().query( geo.id ).filter( and_(ObjectInstance.id == obj_id, ObjectInstance.absolute_description_id == geo.abstraction_desc, geo.type == type) )

def get_geometry(obj_id, geo, type):
  return db().query( geo.id ).filter( and_(ObjectInstance.id == obj_id, ObjectInstance.absolute_description_id == geo.geometry_desc, geo.type == type) )

def get_abstraction(obj_id, geo, type):
  return db().query( geo.id ).filter( and_(ObjectInstance.id == obj_id, ObjectInstance.absolute_description_id == geo.abstraction_desc, geo.type == type) )
