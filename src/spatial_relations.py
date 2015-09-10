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
from semap.ros_postgis_conversion import *

from sqlalchemy.types import UserDefinedType
from sqlalchemy import desc
try:
    from sqlalchemy.sql.functions import _FunctionGenerator
except ImportError:  # SQLA < 0.9
    from sqlalchemy.sql.expression import _FunctionGenerator


from semap_ros.subqueries import *
from semap_ros.instance_srv_calls import *
from semap_msgs.msg import ObjectPair

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
        ids = db().query( obj.id ).filter( obj.id.in_( obj_ids ), \
                                           obj.absolute_description_id == geo.abstraction_desc, geo.type == req.geometry_type, \
                                           ST_DWithin(fromPoint2D(req.point), geo.geometry, req.distance)
                                          ).all()

  res.ids = [id for id, in ids]
  return res

def get_distance_between_objects3d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_distance_between_objects3d" )
  res = GetDistanceBetweenObjectsResponse()

  ref_obj = aliased( ObjectInstance )
  ref_geo = aliased( GeometryModel )

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  if req.reference_object_geometry_type not in ["Position3D", "AxisAligned3D", "BoundingBox",  "BoundingHull", "Body"]:
    rospy.logerror("SEMAP DB SRVs: get_distance_between_objects3d was called with %s which is not a valid 2D geometry type" % req.reference_object_geometry_type)
  else:
    if req.reference_object_types:
      ref_ids = any_obj_types_ids(ref_obj, req.reference_object_types)
    else:
      ref_ids = any_obj_ids(ref_obj)

    if req.target_object_types:
      tar_ids = any_obj_types_ids(tar_obj, req.target_object_types)
    else:
      tar_ids = any_obj_ids(tar_obj)

    if req.max_distance:
      distance = ST_3DMaxDistance(ref_geo.geometry, tar_geo.geometry)
      line = ST_3DLongestLine(ref_geo.geometry, tar_geo.geometry)
    else:
      distance = ST_3DDistance(ref_geo.geometry, tar_geo.geometry)
      line = ST_3DShortestLine(ref_geo.geometry, tar_geo.geometry)

    if req.return_points:
      query = db().query( ref_obj.id, tar_obj.id, distance, line).\
                   filter( ref_obj.id.in_( ref_ids ), tar_obj.id.in_( tar_ids ), \
                           ref_obj.absolute_description_id == ref_geo.abstraction_desc,
                           tar_obj.absolute_description_id == tar_geo.abstraction_desc,
                           ref_geo.type == req.reference_object_geometry_type,
                           tar_geo.type == req.target_object_geometry_type )
    else:
      query = db().query( ref_obj.id, tar_obj.id, distance).\
           filter( ref_obj.id.in_( ref_ids ), tar_obj.id.in_( tar_ids ), \
                   ref_obj.absolute_description_id == ref_geo.abstraction_desc,
                   tar_obj.absolute_description_id == tar_geo.abstraction_desc,
                   ref_geo.type == req.reference_object_geometry_type,
                   tar_geo.type == req.target_object_geometry_type )

    if req.min_range:
      query = query.filter( distance > req.min_range )

    if req.min_range:
      query = query.filter( distance < req.max_range )

    if req.sort_descending:
      query = query.order_by( desc(distance) )
    else:
      query = query.order_by( distance )

    results = query.all()

    if req.return_points:
      for i, j, dist, points in results:
        pair = ObjectPair()
        pair.reference_id = i
        pair.target_id = j
        if req.max_distance:
          pair.max_dist = dist
          pair.max_dist_line[0] = toPoint3D( db().execute( ST_PointN( points, 1 ) ).scalar() )
          pair.max_dist_line[1] = toPoint3D( db().execute( ST_PointN( points, 2 ) ).scalar() )
        else:
          pair.min_dist = dist
          pair.min_dist_line[0] = toPoint3D( db().execute( ST_PointN( points, 1 ) ).scalar() )
          pair.min_dist_line[1] = toPoint3D( db().execute( ST_PointN( points, 2 ) ).scalar() )
        res.pairs.append(pair)
    else:
      for i, j, dist in results:
        pair = ObjectPair()
        pair.reference_id = i
        pair.target_id = j
        if req.max_distance:
          pair.max_dist = dist
        else:
          pair.min_dist = dist
        res.pairs.append(pair)

  return res

def get_objects_within_range( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_range" )
  res = GetObjectsWithinRangeResponse()

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  if req.target_object_types:
    tar_ids = any_obj_types_ids(tar_obj, req.target_object_types)
  else:
    tar_ids = any_obj_ids(tar_obj)

  if req.fully_within:
    operator = ST_3DDFullyWithin(fromPoint3D(req.reference_point), tar_geo.geometry, req.distance)
  else:
    operator = ST_3DDWithin(fromPoint3D(req.reference_point), tar_geo.geometry, req.distance)

  results = db().query( tar_obj.id, ST_3DDistance( fromPoint3D(req.reference_point), tar_geo.geometry), ST_3DMaxDistance( fromPoint3D(req.reference_point), tar_geo.geometry), ST_3DClosestPoint(tar_geo.geometry, fromPoint3D(req.reference_point)) ).\
                  filter( tar_obj.id.in_( tar_ids ), tar_obj.absolute_description_id == tar_geo.abstraction_desc, tar_geo.type == req.target_object_geometry_type, operator).\
                  order_by( ST_3DDistance( fromPoint3D(req.reference_point), tar_geo.geometry) ).all()

  for i, min, max, point in results:
    print i, min, max, point
    pair = ObjectPair()
    pair.reference_id = -1
    pair.target_id = i
    pair.max_dist = max
    pair.max_dist_line[0] = req.reference_point
    pair.max_dist_line[1] = toPoint3D( point )
    pair.min_dist = min
    pair.min_dist_line[0] = req.reference_point
    pair.min_dist_line[1] = toPoint3D( point )
    res.pairs.append(pair)

  return res

def get_directional_relations2d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_directional_relations2d" )
  res = GetDirectionalRelations2DResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerr("SEMAP DB SRVs: get_directional_relations2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
      rospy.loginfo("SEMAP DB SRVs: get_directional_relations2d tries to find em")

      if db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo2.geometry.above(geo1.geometry) ).count():
        res.relations.append("gm_strict-above-of")

      elif db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo2.geometry.overlaps_or_above(geo1.geometry) ).count():
        res.relations.append("gm_relaxed-above-of")


      if db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo2.geometry.below(geo1.geometry) ).count():
        res.relations.append("gm_strict-below-of")


      elif db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo2.geometry.overlaps_or_below(geo1.geometry) ).count():
        res.relations.append("gm_relaxed-below-of")

      if db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo2.geometry.to_left(geo1.geometry) ).count():
        res.relations.append("gm_strict-left-of")

      elif db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo2.geometry.overlaps_or_to_left(geo1.geometry) ).count():
        res.relations.append("gm_relaxed-left-of")

      if db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo2.geometry.to_right(geo1.geometry) ).count():
        res.relations.append("gm_strict-right-of")

      elif db().query( obj1.id, obj2.id ).filter(obj1.id == req.reference_id, \
                                         obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == req.geometry_type, \
                                         obj2.id ==  req.target_id, \
                                         obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == req.geometry_type,
                                         geo2.geometry.overlaps_or_to_right(geo1.geometry) ).count():
        res.relations.append("gm_relaxed-right-of")

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


#string[] reference_object_types
#int32[] reference_object_ids
#string[] target_object_types
#---
#int32[] target_object_ids

def get_objects_within_object( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_object" )
  res = GetObjectsWithinObjectResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if req.target_object_types:
    obj2_ids = any_obj_types_ids(obj2, req.target_object_types)
  else:
    obj2_ids = any_obj_ids(obj2)

  print req.reference_object_id
  print req.target_object_types
  print obj2_ids.all()
  rospy.loginfo("SEMAP DB SRVs: test_containment_relations3d tries to find em")

  ids = db().query( obj2.id ).\
          filter(obj1.id == req.reference_object_id, obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                 obj2.id.in_( obj2_ids ), obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox", \
                 SFCGAL_Contains3D( geo1.geometry, geo2.geometry)
                ).all()

  print ids
  res.target_object_ids = [id for id, in ids]

  return res

def get_containment_pairs( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_containment_pairs" )
  res = GetObjectLocation()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )

  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if req.reference_object_types:
    obj1_ids = any_obj_types_ids(obj1, req.refrence_object_types)
  else:
    obj1_ids = any_obj_ids(obj1)

  if req.target_object_types:
    obj2_ids = any_obj_types_ids(obj2, req.target_object_types)
  else:
    obj2_ids = any_obj_ids(obj2)

  ids = db().query( obj1.id, obj2.id ).\
          filter(obj1.id.in_( obj1_ids ), obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                 obj2.id.in_( obj2_ids ), obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox", \
                 SFCGAL_Contains3D( geo1.geometry, geo2.geometry)
                ).all()

  for i1, i2 in ids:
    pair = ObjectPair()
    pair.reference_id = i1
    pair.relations.append("contains")
    pair.target_id = i2

  res.target_object_ids = [id for id, in ids]

  return res

def unbind_contained_objects(req):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_object" )
  res = GetObjectsWithinObjectResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if req.target_object_types:
    obj2_ids = any_obj_types_ids(obj2, req.target_object_types)
  else:
    obj2_ids = any_obj_ids(obj2)

  print req.reference_object_id
  print req.target_object_types
  print obj2_ids.all()
  rospy.loginfo("SEMAP DB SRVs: test_containment_relations3d tries to find em")

  ids = db().query( obj2.id ).\
          filter(obj1.id == req.reference_object_id, obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                 obj1.frame_id == FrameNode.id,
                 obj2.id.in_( obj2_ids ), obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox", \
                 SFCGAL_Contains3D( geo1.geometry, geo2.geometry)
                ).all()

  res.target_object_ids = [id for id, in ids]

  for id in res.target_object_ids:
    call_change_frame(id, "world", False)

  return res

def bind_contained_objects( req):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_object" )
  res = GetObjectsWithinObjectResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if req.target_object_types:
    obj2_ids = any_obj_types_ids(obj2, req.target_object_types)
  else:
    obj2_ids = any_obj_ids(obj2)

  print req.reference_object_id
  print req.target_object_types
  print obj2_ids.all()
  rospy.loginfo("SEMAP DB SRVs: test_containment_relations3d tries to find em")

  ids = db().query( obj2.id ).\
          filter(obj1.id == req.reference_object_id, obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                 obj1.frame_id == FrameNode.id,
                 obj2.id.in_( obj2_ids ), obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox", \
                 SFCGAL_Contains3D( geo1.geometry, geo2.geometry)
                ).all()

  frame_name = db().query( FrameNode.name ).\
          filter(obj1.id == req.reference_object_id, obj1.frame_id == FrameNode.id).scalar()

  print 'frame_name', frame_name

  res.target_object_ids = [id for id, in ids]

  if frame_name:
    for id in res.target_object_ids:
      call_change_frame(id, frame_name, False)

  return res

def test_containment_relations3d( req ):
  rospy.loginfo( "SEMAP DB SRVs: test_containment_relations3d" )
  res = GetDirectionalRelations2DResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  geos = db().query(  SFCGAL_Contains3D( geo1.geometry, geo2.geometry) ).\
          filter(obj1.id == req.reference_id, \
                 obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                 obj2.id == req.target_id, \
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
    rospy.logerror("SEMAP DB SRVs: get_directional_relations3d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
    rospy.loginfo("SEMAP DB SRVs: get_directional_relation32d tries to find em")

    projection_relations = {}
    projection_relations["infront-of"] = ["FrontProjection"]
    projection_relations["behind-of"]  = ["BackProjection"]
    projection_relations["left-of"]    = ["LeftProjection"]
    projection_relations["right-of"]   = ["RightProjection"]
    projection_relations["above-of"]   = ["TopProjection"]
    projection_relations["below-of"]   = ["BotProjection"]

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
        res.relations.append("ps_strict-" + relation)

      if relaxed:
        res.relations.append("ps_relaxed-" + relation)

    halfspace_relations = {}
    halfspace_relations["infront-of"] = ["FrontHalfspace"]
    halfspace_relations["behind-of"]  = ["BackHalfspace"]
    halfspace_relations["left-of"]    = ["LeftHalfspace"]
    halfspace_relations["right-of"]   = ["RightHalfspace"]
    halfspace_relations["above-of"]   = ["TopHalfspace"]
    halfspace_relations["below-of"]   = ["BotHalfspace"]

    for relation in halfspace_relations.keys():

      for region in halfspace_relations[relation]:

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
        res.relations.append("hs_strict-" + relation)

      if relaxed :
        res.relations.append("hs_relaxed-" + relation)

  return res

def get_objects_on_obb3d( req ):

  rospy.loginfo( "SEMAP DB SRVs: get_objects_on_obb3d" )

  res = GetObjectsOnOBB3DResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )
  geo3 = aliased( GeometryModel )

  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  #if req.geometry_type not in ["BoundingBox"]:
  #  rospy.logerror("SEMAP DB SRVs: get_objects_on_obb3d was called with %s which is not a valid D geometry type" % req.geometry_type)
  #else:
  #    rospy.loginfo("SEMAP DB SRVs: get_objects_on_obb3d tries to find em")

  if req.reference_object_types:
    obj1_ids = any_obj_types_ids(obj1, req.reference_object_types).all()
  else:
    obj1_ids = any_obj_ids(obj1).all()

  print 'ref', obj1_ids

  print req.target_object_types

  if req.target_object_types:
    obj2_ids = any_obj_types_ids(obj2, req.target_object_types).all()
  else:
    obj2_ids = any_obj_ids(obj2).all()

  print 'target', obj2_ids

  ids = db().query( obj1.id, obj2.id ).filter( obj1.id.in_( obj1_ids ), obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                                               obj1.id.in_( obj1_ids ), obj1.absolute_description_id == geo3.abstraction_desc, geo3.type == "TopProjection", \
                                               obj2.id.in_( obj2_ids ), obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox", \
                                               SFCGAL_Contains3D( geo3.geometry, geo2.geometry),
                                               SFCGAL_Distance3D( geo1.geometry, geo2.geometry) < 0.05
                                            ).all()
  for i, d in ids:
    pair = ObjectPair()
    pair.reference_id = i
    pair.target_id = d
    pair.relations.append("supports")
    res.pairs.append(pair)
    pair = ObjectPair()
    pair.reference_id = d
    pair.target_id = i
    pair.relations.append("is-on")
    res.pairs.append(pair)

  return res

def get_objects_in_obb3d( req ):

  rospy.loginfo( "SEMAP DB SRVs: get_objects_in_obb3d" )

  res = GetObjectsOnOBB3DResponse()

  obj1 = aliased( ObjectInstance )
  geo1 = aliased( GeometryModel )

  obj2 = aliased( ObjectInstance )
  geo2 = aliased( GeometryModel )

  if req.reference_object_types:
    obj1_ids = any_obj_types_ids(obj1, req.reference_object_types).all()
  else:
    obj1_ids = any_obj_ids(obj1).all()

  if req.target_object_types:
    obj2_ids = any_obj_types_ids(obj2, req.target_object_types).all()
  else:
    obj2_ids = any_obj_ids(obj2).all()

  ids = db().query( obj1.id, obj2.id ).filter( obj1.id.in_( obj1_ids ), obj1.absolute_description_id == geo1.abstraction_desc, geo1.type == "BoundingBox", \
                                               obj2.id.in_( obj2_ids ), obj2.absolute_description_id == geo2.abstraction_desc, geo2.type == "BoundingBox", \
                                               SFCGAL_Contains3D( geo1.geometry, geo2.geometry)
                                            ).all()
  for i, d in ids:
    pair = ObjectPair()
    pair.reference_id = i
    pair.target_id = d
    pair.relations.append("contains")
    res.pairs.append(pair)
    pair = ObjectPair()
    pair.reference_id = d
    pair.target_id = i
    pair.relations.append("contained-by")
    res.pairs.append(pair)

  return res

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
  '''
