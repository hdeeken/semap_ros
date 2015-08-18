#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'spatial_db_ros' )

from sqlalchemy.orm import aliased, join
from sqlalchemy import or_
from geoalchemy2 import comparator 
from db_model import *
from sqlalchemy import func
from db_environment import db
from spatial_db_ros.srv import *
from spatial_db.ros_postgis_conversion import *

from sqlalchemy.types import UserDefinedType
try:
    from sqlalchemy.sql.functions import _FunctionGenerator
except ImportError:  # SQLA < 0.9
    from sqlalchemy.sql.expression import _FunctionGenerator


from spatial_db_ros.subqueries import *

'''
SEMAP Spatial Relations Services
'''

def get_objects_within_polygon2d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_polygon2d" )
  res = GetObjectsWithinPolygon2DResponse()

  obj = aliased( ObjectInstance )
  geo = aliased( GeometryModel )

  if req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerror("SEMAP DB SRVs: get_objects_within_polygon2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
      rospy.loginfo("SEMAP DB SRVs: get_objects_within_polygon2d tries to find em")
      #ids = db().query( obj.id ).join(ObjectInstance.id).filter(
      #  obj.id.in_( any_obj_types_ids( obj, req.object_types ) ),
      #  geo.id.in_( get_abstraction( obj.id, geo, req.geometry_type ) ),
      #  ST_Within( geo.geometry, fromPolygon2D(req.polygon) ) ).all()

      if req.object_types:
        obj_ids = any_obj_types_ids(obj, req.object_types)
      else:
        obj_ids = any_obj_ids(obj)

      if req.fully_within:
        ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                           obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                           ST_Within(geo.geometry, fromPolygon2D(req.polygon) )
                                          ).all()
      else:
        ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                           obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                           or_(ST_Overlaps(fromPolygon2D(req.polygon), geo.geometry  ),
                                              ST_Within(geo.geometry, fromPolygon2D(req.polygon) ) )
                                          ).all()

      
  res.ids = [id for id, in ids]
  return res

def get_objects_within_range2d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_range2d" )
  res = GetObjectsWithinRange2DResponse()

  obj = aliased( ObjectInstance )
  geo = aliased( GeometryModel )

  print req.object_types, req.point, req.geometry_type, req.distance

  if req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerror("SEMAP DB SRVs: get_objects_within_range2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
      rospy.loginfo("SEMAP DB SRVs: get_objects_within_range2d tries to find em")

      if req.object_types:
        obj_ids = any_obj_types_ids(obj, req.object_types)
      else:
        obj_ids = any_obj_ids(obj)

      if req.fully_within:
        ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                           obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                           ST_DFullyWithin(fromPoint2D(req.point),geo.geometry, req.distance)
                                          ).all()
      else:
        # use ST_Buffer to create a circle and check for overlap?
        ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                           obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                           ST_DWithin(fromPoint2D(req.point), geo.geometry, req.distance)
                                          ).all()
        
  res.ids = [id for id, in ids]
  return res

def get_objects_within_range3d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_range3d" )
  res = GetObjectsWithinRange2DResponse()

  obj = aliased( ObjectInstance )
  geo = aliased( GeometryModel )

  print req.object_types, req.point, req.geometry_type, req.distance

  if req.geometry_type not in ["Position3D", "AxisAligned3D", "BoundingBox",  "BoundingHull", "Body"]:
    rospy.logerror("SEMAP DB SRVs: get_objects_within_range3d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
      rospy.loginfo("SEMAP DB SRVs: get_objects_within_range3d tries to find em")

      if req.object_types:
        obj_ids = any_obj_types_ids(obj, req.object_types)
      else:
        obj_ids = any_obj_ids(obj)

      if req.geometry_type == "Body":
        if req.fully_within:
          ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                             obj.absolute_description_id == geo.geometry_desc, geo.type == req.geometry_type, \
                                             ST_3DDFullyWithin(fromPoint3D(req.point), geo.geometry, req.distance)
                                            ).all()
        else:
          # use ST_Buffer to create a circle and check for overlap?
          ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                             obj.absolute_description_id == geo.geometry_desc, geo.type == req.geometry_type, \
                                             ST_3DDWithin(fromPoint2D(req.point), geo.geometry, req.distance)
                                            ).all()
      else:
        if req.fully_within:
          ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                             obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                             ST_3DDFullyWithin(fromPoint3D(req.point), geo.geometry, req.distance)
                                            ).all()
        else:
          # use ST_Buffer to create a circle and check for overlap?
          ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                             obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                             ST_3DDWithin(fromPoint2D(req.point), geo.geometry, req.distance)
                                            ).all()
        
  res.ids = [id for id, in ids]
  return res

def get_objects_within_range3d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_range3d" )
  res = GetObjectsWithinRange2DResponse()

  obj = aliased( ObjectInstance )
  geo = aliased( GeometryModel )

  print req.object_types, req.point, req.geometry_type, req.distance

  if req.geometry_type not in ["Position3D", "AxisAligned3D", "BoundingBox",  "BoundingHull", "Body"]:
    rospy.logerror("SEMAP DB SRVs: get_objects_within_range3d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
      rospy.loginfo("SEMAP DB SRVs: get_objects_within_range3d tries to find em")

      if req.object_types:
        obj_ids = any_obj_types_ids(obj, req.object_types)
      else:
        obj_ids = any_obj_ids(obj)

      if req.geometry_type == "Body":
        if req.fully_within:
          ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                             obj.absolute_description_id == geo.geometry_desc, geo.type == req.geometry_type, \
                                             ST_3DDFullyWithin(fromPoint3D(req.point), geo.geometry, req.distance)
                                            ).all()
        else:
          # use ST_Buffer to create a circle and check for overlap?
          ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                             obj.absolute_description_id == geo.geometry_desc, geo.type == req.geometry_type, \
                                             ST_3DDWithin(fromPoint2D(req.point), geo.geometry, req.distance)
                                            ).all()
      else:
        if req.fully_within:
          ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                             obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                             ST_3DDFullyWithin(fromPoint3D(req.point), geo.geometry, req.distance)
                                            ).all()
        else:
          # use ST_Buffer to create a circle and check for overlap?
          ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                             obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                             ST_3DDWithin(fromPoint2D(req.point), geo.geometry, req.distance)
                                            ).all()
    
  res.ids = [id for id, in ids]
  return res

def get_directional_relations2d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_directional_relations2d" )
  res = GetDirectionalRelations2DResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerror("SEMAP DB SRVs: get_directional_relations2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
      rospy.loginfo("SEMAP DB SRVs: get_directional_relations2d tries to find em")

      if db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo1.geometry.above(geo2.geometry) ).count():
        res.relations.append("above_of")

      elif db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo1.geometry.overlaps_or_above(geo2.geometry) ).count():
        res.relations.append("partiall_above_of")


      if db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo1.geometry.below(geo2.geometry) ).count():
        res.relations.append("below_of")


      elif db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo1.geometry.overlaps_or_below(geo2.geometry) ).count():
        res.relations.append("partially_below_of")

      if db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo1.geometry.to_left(geo2.geometry) ).count():
        res.relations.append("left_of")

      elif db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo1.geometry.overlaps_or_to_left(geo2.geometry) ).count():
        res.relations.append("partially_left_of")

      if db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo1.geometry.to_right(geo2.geometry) ).count():
        res.relations.append("right_of")

      elif db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo1.geometry.overlaps_or_to_right(geo2.geometry) ).count():
        res.relations.append("partially_right_of")

  return res


def test_directional_relations2d( req ):
  rospy.loginfo( "SEMAP DB SRVs: test_directional_relations2d" )
  res = GetDirectionalRelations2DResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerror("SEMAP DB SRVs: get_directional_relations2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
      rospy.loginfo("SEMAP DB SRVs: get_directional_relations2d tries to find em")

      obj_ids1 = any_obj_types_ids(obj1, ["ConferenceChair"])
      obj_ids2 = any_obj_types_ids(obj2, ["ConferenceChair"])

      #pairs = db().query( obj1.id, obj2.id).filter(obj1.id.in_( obj_ids1 ), \
                                         #obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         #obj2.id.in_( obj_ids2 ), \
                                         #obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         #geo1.geometry.to_left(geo2.geometry) ).all()
