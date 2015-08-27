#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'semap_ros' )

from sqlalchemy.orm import aliased, join
from sqlalchemy import or_
from geoalchemy2 import comparator
from db_model import *
from sqlalchemy import func
from db_environment import db
from semap_ros.srv import *
from spatial_db.ros_postgis_conversion import *

from sqlalchemy.types import UserDefinedType
try:
    from sqlalchemy.sql.functions import _FunctionGenerator
except ImportError:  # SQLA < 0.9
    from sqlalchemy.sql.expression import _FunctionGenerator


from semap_ros.subqueries import *

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
        res.relations.append("partially_above_of")


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


def test_containment_relations3d( req ):
  rospy.loginfo( "SEMAP DB SRVs: test_containment_relations3d" )
  res = GetDirectionalRelations2DResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  rospy.loginfo("SEMAP DB SRVs: test_containment_relations3d tries to find em")

  geos = db().query(  SFCGAL_Contains3D( geo1.geometry, geo2.geometry) ).\
          filter(obj1.id == req.reference_id, \
                 obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                 obj2.id ==  req.target_id, \
                 obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox" ).all()

  print geos
  ''''
  for geoI, geoII in geos:
    print 'geoI', db().execute( ST_AsText( geoI.geometry) ).scalar()
    #print 'isValid', db().execute( SFCGAL_IsValid( geoI.geometry) ).scalar()
    print 'geoI', db().execute( ST_AsText( geoII.geometry) ).scalar()
    #print 'isValid', db().execute( SFCGAL_IsValid( geoII.geometry) ).scalar()
    containment_status = db().execute( SFCGAL_Contains3D( geoI.geometry, geoII.geometry ) ).scalar()
    print 'containment_status:', containment_status

  #if containment_status:
  #  rospy.loginfo("OBJECT CONTAINMENT VERIFIED!")
  #else:
  #  rospy.loginfo("OBJECT CONTAINMENT REJECTED!")
  '''
  return res

def get_directional_relations3d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_directional_relations3d" )
  res = GetDirectionalRelations2DResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if False: #req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerror("SEMAP DB SRVs: get_directional_relations2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
    rospy.loginfo("SEMAP DB SRVs: get_directional_relations2d tries to find em")

    '''
    "FrontExtrusion"
    "FrontRightExtrusion"
    "FrontRightTopExtrusion"
    "FrontRightBotExtrusion"
    "FrontLeftExtrusion"
    "FrontLeftTopExtrusion"
    "FrontLeftBotExtrusion"
    "FrontTopExtrusion"
    "FrontBotExtrusion"
    "RightExtrusion"
    "RightTopExtrusion"
    "RightBotExtrusion"
    "LeftExtrusion"
    "LeftTopExtrusion"
    "LeftBotExtrusion"
    "TopExtrusion"
    "BotExtrusion"
    "BackExtrusion"
    "BackRightExtrusion"
    "BackRightTopExtrusion"
    "BackRightBotExtrusion"
    "BackTopExtrusion"
    "BackBotExtrusion"
    "BackLeftExtrusion"
    "BackLeftTopExtrusion"
    "BackLeftBotExtrusion"
    '''

    projection_relations = {}
    projection_relations["infront-of"] = ["FrontExtrusion"]
    projection_relations["behind-of"] = ["BackExtrusion"]
    projection_relations["left-of"] = ["LeftExtrusion"]
    projection_relations["right-of"] = ["RightExtrusion"]
    projection_relations["above-of"] = ["TopExtrusion"]
    projection_relations["below-of"] = ["BotExtrusion"]

    for relation in projection_relations.keys():

      for region in projection_relations[relation]:

        strict = db().query(  SFCGAL_Contains3D( geo1.geometry, geo2.geometry) ).\
              filter(obj1.id == req.reference_id, \
                     obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == region, \
                     obj2.id ==  req.target_id, \
                     obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox" ).scalar()

        relaxed = db().query(  SFCGAL_Intersects3D( geo1.geometry, geo2.geometry) ).\
              filter(obj1.id == req.reference_id, \
                     obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == region, \
                     obj2.id ==  req.target_id, \
                     obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox" ).scalar()

      if strict:
        res.relations.append("proj_strictly" + relation)

      if relaxed:
        res.relations.append("proj_relaxed" + relation)

    halfspace_relations = {}

    halfspace_relations["infront-of"] = \
      ["FrontExtrusion", "FrontLeftExtrusion", "FrontRightExtrusion", "FrontTopExtrusion", "FrontBotExtrusion", "FrontLeftTopExtrusion", "FrontLeftBotExtrusion", "FrontRightTopExtrusion", "FrontRightBotExtrusion"]
    halfspace_relations["behind-of"] = \
      ["BackExtrusion", "BackLeftExtrusion", "BackRightExtrusion", "BackTopExtrusion", "BackBotExtrusion", "BackLeftTopExtrusion", "BackLeftBotExtrusion", "BackRightTopExtrusion", "BackRightBotExtrusion"]
    halfspace_relations["left-of"] = \
      ["FrontLeftExtrusion", "FrontLeftTopExtrusion", "FrontLeftBotExtrusion", "LeftExtrusion", "LeftTopExtrusion", "LeftBotExtrusion", "BackLeftExtrusion", "BackLeftTopExtrusion", "BackLeftBotExtrusion"]
    halfspace_relations["right-of"] = \
      ["FrontRightExtrusion", "FrontRightTopExtrusion", "FrontRightBotExtrusion", "RightExtrusion", "RightTopExtrusion", "RightBotExtrusion", "BackRightExtrusion", "BackRightTopExtrusion", "BackRightBotExtrusion"]
    halfspace_relations["above-of"] = \
      ["FrontRightTopExtrusion", "FrontLeftTopExtrusion", "FrontTopExtrusion", "RightTopExtrusion", "LeftTopExtrusion", "TopExtrusion", "BackRightTopExtrusion", "BackTopExtrusion", "LeftTopExtrusion"]
    halfspace_relations["below-of"] = \
      ["FrontRightBotExtrusion", "FrontLeftBotExtrusion", "FrontBotExtrusion", "RightBotExtrusion", "LeftBotExtrusion", "BotExtrusion", "BackRightBotExtrusion", "BackBotExtrusion", "LeftBotExtrusion"]

    for relation in halfspace_relations.keys():
      strict = False
      relaxed = False
      for region in halfspace_relations[relation]:

        strict = strict or db().query(  SFCGAL_Contains3D( geo1.geometry, geo2.geometry) ).\
              filter(obj1.id == req.reference_id, \
                     obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == region, \
                     obj2.id ==  req.target_id, \
                     obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox" ).scalar()

        relaxed = relaxed or db().query(  SFCGAL_Intersects3D( geo1.geometry, geo2.geometry) ).\
              filter(obj1.id == req.reference_id, \
                     obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == region, \
                     obj2.id ==  req.target_id, \
                     obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox" ).scalar()

      if strict:
        res.relations.append("hs_strictly-" + relation)

      if relaxed:
        res.relations.append("hs_relaxed-" + relation)

  return res
'''
      distance = db().query( SFCGAL_Distance3D( geo1.geometry, geo2.geometry) ).\
            filter(obj1.id == req.reference_id, \
                   obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                   obj2.id ==  req.target_id, \
                   obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox" ).scalar()
      print distance
      if distance < 0.05:
        res.relations.append("on")
'''
