#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest('semap_ros')

from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased, join

from semap.db_model import *
from semap.db_environment import db

from semap_ros.srv import *
from semap.ros_postgis_conversion import *

#int32[] ids
#string[] class_types
#string[] superclass_types
#string[] attributes

#string geometry_type
#bool intra_object_analysis
'''
def apply_object_constraints(obj, ids, class_types):
  query = db().query( obj.id ).filter( or_( obj.id.in_( ids ), obj.relative_description_id == desc.id, or_(*[desc.type.contains(type) for type in class_types]) ) )

def apply_geometry_constraints(obj, geo ids, class_types):
  query = db().query( obj.id ).filter( ref_obj.absolute_description_id == ref_geo.abstraction_desc, ref_geo.type == req.reference_object_geometry_type )
'''
'''
def get_obj_geo( contraint )
  obj = aliased( ObjectInstance )
  geo = aliased( GeometryModel )

  if req.reference_object_types:
    ref_obj_ids = any_obj_types_ids(ref_obj, req.reference_object_types).all()
  else:
    ref_obj_ids = any_obj_ids(ref_obj).all()

  ref_obj.id == req.reference_object_id, ref_obj.absolute_description_id == ref_geo.abstraction_desc, ref_geo.type == "BoundingBox",
  return obj, geo
'''
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

def constrain_obj_ids(obj, obj_constraint):
  desc = aliased( ObjectDescription )
  no_ids = False
  no_class = False
  no_super = False
  query_constraint = None
  if obj_constraint.ids and obj_constraint.ids != (0,):
    query_constraint = obj.id.in_( obj_constraint.ids )
  else:
    no_ids = True
  if obj_constraint.class_types and obj_constraint.class_types != ['']:
    query_constraint = or_(query_constraint, or_(*[desc.type == type for type in obj_constraint.class_types]) )
  else:
    no_class = True

  if obj_constraint.superclass_types and obj_constraint.superclass_types != ['']:
    query_constraint = or_(query_constraint, or_(*[desc.type.contains(type) for type in obj_constraint.superclass_types]) )
  else:
    no_super = True

  if no_ids and no_class and no_super:
    return db().query( obj.id )
  else:
    return db().query( obj.id ).\
      filter( obj.relative_description_id == desc.id,
              query_constraint )

def constrain_geo_ids(obj, geo, obj_constraint):
  return db().query( geo.id ).\
    filter( obj.id.in_( constrain_obj_ids(obj, obj_constraint) ),
            obj.absolute_description_id == geo.abstraction_desc,
            geo.type == obj_constraint.geometry_type )

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
