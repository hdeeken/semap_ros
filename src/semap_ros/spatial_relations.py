#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'semap_ros' )

from sqlalchemy import or_, func, desc
from sqlalchemy.orm import aliased, join
from sqlalchemy.types import UserDefinedType
try:
    from sqlalchemy.sql.functions import _FunctionGenerator
except ImportError:  # SQLA < 0.9
    from sqlalchemy.sql.expression import _FunctionGenerator

from geoalchemy2 import comparator

from semap.db_model import *
from semap.db_environment import db

from semap_ros.srv import *
from semap.ros_postgis_conversion import *
from semap_ros.subqueries import *
from semap_ros.instance_srv_calls import *

from semap_msgs.msg import ObjectPair, OneToMany

'''
SEMAP Spatial Relations Services
'''

###
### METRIC OPERATORS
###

def get_objects_within_range2d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_range2d" )
  res = GetObjectsWithinRange2DResponse()

  obj = aliased( ObjectInstance )
  geo = aliased( GeometryModel )

  print req.object_types, req.point, req.geometry_type, req.distance

  if req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerr("SEMAP DB SRVs: get_objects_within_range2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
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

def get_distance_between_objects( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_distance_between_objects3d" )
  res = GetDistanceBetweenObjectsResponse()

  ref_obj = aliased( ObjectInstance )
  ref_geo = aliased( GeometryModel )

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  if req.reference_object_geometry_type not in ["Position3D", "AxisAligned3D", "BoundingBox",  "BoundingHull", "Body"]:
    rospy.logerr("SEMAP DB SRVs: get_distance_between_objects3d was called with %s which is not a valid 2D geometry type" % req.reference_object_geometry_type)
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

#string[] reference_object_types
#int32[] reference_object_ids
#string[] target_object_types
#---
#int32[] target_object_ids

###
### TOPOLOGICAL OPERATORS
###

def get_objects_within_area( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_area" )
  res = GetObjectsWithinAreaResponse()

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  if req.target_object_types:
    tar_ids = any_obj_types_ids(tar_obj, req.target_object_types)
  else:
    tar_ids = any_obj_ids(tar_obj)

  if req.fully_within:
    operator = ST_Within( tar_geo.geometry, fromPolygon2D(req.reference_polygon))
  else:
    operator =  or_(ST_Overlaps(fromPolygon2D(req.reference_polygon), tar_geo.geometry),
                    ST_Within(tar_geo.geometry, fromPolygon2D(req.reference_polygon) ) )

  results = db().query( tar_obj.id ).\
                 filter( tar_obj.id.in_( tar_ids ),
                         tar_obj.absolute_description_id == tar_geo.abstraction_desc,
                         tar_geo.type == req.target_object_geometry_type,
                         operator).all()

  for i in results:
    pair = ObjectPair()
    pair.reference_id = -1
    pair.target_id = i[0]
    pair.relations.append("contained-in-area")
    res.pairs.append(pair)
  return res

def get_objects_within_volume( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_area" )
  res = GetObjectsWithinVolumeResponse()

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  if req.target_object_types:
    tar_ids = any_obj_types_ids(tar_obj, req.target_object_types)
  else:
    tar_ids = any_obj_ids(tar_obj)

  if req.fully_within:
    operator = SFCGAL_Contains3D(fromPolygonMesh3D(req.reference_mesh), tar_geo.geometry)
  else:
    operator = or_( SFCGAL_Contains3D(fromPolygonMesh3D(req.reference_mesh), tar_geo.geometry),
                    SFCGAL_Intersects3D(fromPolygonMesh3D(req.reference_mesh), tar_geo.geometry) )

  results = db().query( tar_obj.id ).\
                 filter( tar_obj.id.in_( tar_ids ),
                         tar_obj.absolute_description_id == tar_geo.abstraction_desc,
                         tar_geo.type == req.target_object_geometry_type,
                         operator).all()

  for i in results:
    pair = ObjectPair()
    pair.reference_id = -1
    pair.target_id = i[0]
    pair.relations.append("contained-in-volume")
    res.pairs.append(pair)

  return res
  '''
  obj = aliased( ObjectInstance )
  geo = aliased( GeometryModel )

  if req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerr("SEMAP DB SRVs: get_objects_within_polygon2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
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
  '''

def get_objects_within_object( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_object" )
  res = GetObjectsWithinObjectResponse()

  ref_obj = aliased( ObjectInstance )
  ref_geo = aliased( GeometryModel )
  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  if req.target_object_types:
    tar_ids = any_obj_types_ids(tar_obj, req.target_object_types)
  else:
    tar_ids = any_obj_ids(tar_obj)

  if req.fully_within:
    operator = SFCGAL_Contains3D(ref_geo.geometry, tar_geo.geometry)
  else:
    operator = or_( SFCGAL_Contains3D(ref_geo.geometry, tar_geo.geometry),
                    SFCGAL_Intersects3D(ref_geo.geometry, tar_geo.geometry) )

  ids = db().query( tar_obj.id ).\
             filter( ref_obj.id == req.reference_object_id, ref_obj.absolute_description_id == ref_geo.abstraction_desc, ref_geo.type == "BoundingBox",
                     tar_obj.id.in_( tar_ids ), tar_obj.absolute_description_id == tar_geo.abstraction_desc, tar_geo.type == "BoundingBox",
                     operator ).all()

  res.target_object_ids = [id for id, in ids]

  return res

def get_objects_in_objects( req ):

  then = rospy.Time.now()
  rospy.logdebug( "SEMAP DB SRVs: get_objects_in_objects" )
  res = GetObjectsInObjectsResponse()

  if req.reference.geometry_type in [ "AxisAligned3D", "BoundingBox", "Position3D" ]: #,  "BoundingHull"
    ref_status = "3D"
  elif req.reference.geometry_type in [ "Position2D", "FootprintBox", "FootprintHull" ]:
    ref_status = "2D"
  else:
    rospy.logerr("SEMAP DB SRVs: get_objects_in_objects was called with %s as reference geometry thats currently not supported",  req.reference.geometry_type)
    return res

  if req.target.geometry_type in [ "AxisAligned3D", "BoundingBox", "Position3D"]: #,  "BoundingHull"
    tar_status = "3D"
  elif req.target.geometry_type in [ "Position2D", "FootprintBox", "FootprintHull" ]:
    tar_status = "2D"
  else:
    rospy.logerr("SEMAP DB SRVs: get_objects_in_objects was called with %s as target geometry thats currently not supported",  req.target.geometry_type)
    return res

  if ref_status != tar_status:
    rospy.logerr("SEMAP DB SRVs: get_objects_in_objects was called with requests of inconsistent dimensionality")
    return res

  ref_obj = aliased( ObjectInstance )
  ref_geo = aliased( GeometryModel )

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  ##print 'identify references'
  #if req.reference.ids and req.reference.ids != (0,):
    ##print 'by ids'
    #ref_ids = req.reference.ids
  #elif req.reference.class_types:
    ##print 'by class'
    #ref_ids = any_obj_types_ids( ref_obj, req.reference.class_types ).all()
  #else:
    ##print 'just all'
    #ref_ids = any_obj_ids( ref_obj ).all()

  ##print 'ref', ref_ids

  ##print 'identify targets'
  #if req.target.ids and req.target.ids != (0,):
    ##print 'by ids'
    #tar_ids = req.target.ids
  #elif req.target.class_types:
    ##print 'by class'
    #tar_ids = any_obj_types_ids( tar_obj, req.target.class_types ).all()
  #else:
    ##print 'just all'
    #tar_ids = any_obj_ids( tar_obj ).all()

  #print 'tar', tar_ids



  ref_ids =constrain_obj_ids(ref_obj, req.reference).all()
  tar_ids =constrain_obj_ids(tar_obj, req.target).all()

  res.refs =  len(ref_ids)
  res.tars =  len(tar_ids)
  res.num_tests =  len(ref_ids) * len(tar_ids)

  rospy.logwarn( "Testing %d objects of %s containment in %d objects.", len(tar_ids), ref_status, len(ref_ids) )

  if ref_status == "2D":
    if req.fully_within:
      operator = ST_Within(tar_geo.geometry, ref_geo.geometry)
    else:
      operator = or_( ST_Within(tar_geo.geometry, ref_geo.geometry),
                      ST_Overlaps(tar_geo.geometry, ref_geo.geometry) )
  else:
    if req.fully_within:
      operator = SFCGAL_Contains3D(ref_geo.geometry, tar_geo.geometry)
    else:
      operator = or_( SFCGAL_Contains3D(ref_geo.geometry, tar_geo.geometry),
                      SFCGAL_Intersects3D(ref_geo.geometry, tar_geo.geometry) )

  retrieval =  ( rospy.Time.now() - then ).to_sec()

  ids = db().query( ref_obj.id, tar_obj.id ).\
             filter( ref_obj.id.in_( ref_ids ),
                     ref_obj.absolute_description_id == ref_geo.abstraction_desc,
                     ref_geo.type == req.reference.geometry_type,
                     tar_obj.id.in_( tar_ids ),
                     tar_obj.absolute_description_id == tar_geo.abstraction_desc,
                     tar_geo.type == req.target.geometry_type,
                     ref_obj.id != tar_obj.id,
                     operator).order_by(ref_obj.id).all()

  testing = ( rospy.Time.now() - then ).to_sec() - retrieval
  print ids

  if ids:

    old_i = None
    one_to_many = OneToMany()
    #print ids[0][0]
    one_to_many.reference_id = ids[0][0]
    for i, d in ids:
      if one_to_many.reference_id != i:
        res.results.append(one_to_many)
        one_to_many = OneToMany()
        one_to_many.reference_id = i
      one_to_many.target_ids.append(d)
      pair = ObjectPair()
      pair.reference_id = i
      pair.target_id = d
      pair.relations.append("contains")
      res.pairs.append(pair)
    res.results.append(one_to_many)

  packaging =  ( rospy.Time.now() - then ).to_sec() - retrieval - testing

  #rospy.loginfo( "ObjectsInObjects %f:" % ( rospy.Time.now() - then ).to_sec() )
  #rospy.loginfo( "  Retrieving     %f:" % retrieval)
  #rospy.loginfo( "  Testing        %f:" % testing)
  #rospy.loginfo( "  Packaging      %f:" % packaging )

  #print '#pairs', len(res.pairs)

  return res

def get_objects_on_objects( req ):

  rospy.logdebug( "SEMAP DB SRVs: get_objects_on_objects" )
  res = GetObjectsOnObjectsResponse()

  if req.reference.geometry_type not in [ "BoundingBox" ]: #"Position3D",  "BoundingHull"
    rospy.logerr("SEMAP DB SRVs: get_objects_in_objects was called with %s as reference geometry thats currently not supported",  req.reference.geometry_type)
    return res

  if req.target.geometry_type not in [ "BoundingBox" ]: #"Position3D",  "BoundingHull"
    rospy.logerr("SEMAP DB SRVs: get_objects_in_objects was called with %s as target geometry thats currently not supported",  req.reference.geometry_type)
    return res

  ref_obj = aliased( ObjectInstance )
  ref_geo = aliased( GeometryModel )
  ref_top = aliased( GeometryModel )

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  #if req.reference.ids and req.reference.ids != (0,):
    #ref_ids = req.reference.ids
  #elif req.reference.class_types:
    #ref_ids = any_obj_types_ids( ref_obj, req.reference.class_types ).all()
  #else:
    #ref_ids = any_obj_ids( ref_obj ).all()

  #if req.target.ids and req.target.ids != (0,):
    #tar_ids = req.target.ids
  #elif req.target.class_types:
    #tar_ids = any_obj_types_ids( tar_obj, req.target.class_types ).all()
  #else:
    #tar_ids = any_obj_ids( tar_obj ).all()


  ref_ids =constrain_obj_ids(ref_obj, req.reference).all()
  tar_ids =constrain_obj_ids(tar_obj, req.target).all()
  res.refs =  len(ref_ids)
  res.tars =  len(tar_ids)
  res.num_tests =  len(ref_ids) * len(tar_ids)

  print 'ON TESTS', res.num_tests
  print 'refs', ref_ids
  print 'refs', tar_ids

  pairs = db().query( ref_obj.id, tar_obj.id ).\
              filter( ref_obj.id.in_( ref_ids ),
                      ref_obj.absolute_description_id == ref_geo.abstraction_desc,
                      ref_geo.type == req.reference.geometry_type,
                      ref_obj.absolute_description_id == ref_top.abstraction_desc,
                      ref_top.type == "TopProjection",
                      tar_obj.id.in_( tar_ids ),
                      tar_obj.absolute_description_id == tar_geo.abstraction_desc, tar_geo.type == req.target.geometry_type,
                      SFCGAL_Contains3D( ref_top.geometry, tar_geo.geometry),
                      SFCGAL_Distance3D( ref_geo.geometry, tar_geo.geometry) < req.threshold).order_by(ref_obj.id).all()

  if pairs:
    old_ref = None
    one_to_many = OneToMany()
    one_to_many.reference_id = pairs[0][0]
    for ref_id, tar_id in pairs:
      if one_to_many.reference_id != ref_id:
        res.results.append(one_to_many)
        one_to_many = OneToMany()
        one_to_many.reference_id = ref_id
      one_to_many.target_ids.append(tar_id)
      pair = ObjectPair()
      pair.reference_id = ref_id
      pair.target_id = tar_id
      pair.relations.append("contains")
      res.pairs.append(pair)
    res.results.append(one_to_many)

  return res



def test(query, obj, geo, constraint):

  if constraint.ids and constraint.ids != (0,):
    ref_ids = constraint.ids
  elif constraint.class_types:
    ref_ids = any_obj_types_ids( obj, constraint.class_types ).all()
  else:
    ref_ids = any_obj_ids( obj ).all()

  print ref_ids
  print constraint.ids

  return db().query(geo).filter(or_( obj.id.in_( ref_ids ), obj.id.in_( constraint.ids )),
                       obj.absolute_description_id == geo.geometry_desc,
                       geo.type == constraint.geometry_type).subquery()



def get_geo(obj, geo, constraint):

  if constraint.ids and constraint.ids != (0,):
    ref_ids = constraint.ids
  elif constraint.class_types:
    ref_ids = any_obj_types_ids( obj, constraint.class_types ).all()
  else:
    ref_ids = any_obj_ids( obj ).all()

  return db().query(geo).filter(or_( obj.id.in_( ref_ids ), obj.id.in_( constraint.ids )),
                       obj.absolute_description_id == geo.geometry_desc,
                       geo.type == constraint.geometry_type).subquery()

def get_objects_intersect_objects( req ):
  then = rospy.Time.now()
  rospy.logdebug( "SEMAP DB SRVs: get_objects_intersect_objects" )
  res = GetObjectsIntersectObjectsResponse()

  if req.reference.geometry_type in [ "AxisAligned3D", "BoundingBox", "Body"  ]: #"Position3D",  "BoundingHull"
    ref_status = "3D"
  elif req.reference.geometry_type in [ "Position2D", "FootprintBox", "FootprintHull" ]:
    ref_status = "2D"
  else:
    rospy.logerr("SEMAP DB SRVs: get_objects_in_objects was called with %s as reference geometry thats currently not supported",  req.reference.geometry_type)
    return res

  if req.target.geometry_type in [ "AxisAligned3D", "BoundingBox", "Body" ]: #"Position3D",  "BoundingHull"
    tar_status = "3D"
  elif req.target.geometry_type in [ "Position2D", "FootprintBox", "FootprintHull" ]:
    tar_status = "2D"
  else:
    rospy.logerr("SEMAP DB SRVs: get_objects_in_objects was called with %s as target geometry thats currently not supported",  req.target.geometry_type)
    return res

  if ref_status != tar_status:
    rospy.logerr("SEMAP DB SRVs: get_objects_in_objects was called with requests of inconsistent dimensionality")
    return res

  ref_obj = aliased( ObjectInstance )
  ref_geo = aliased( GeometryModel )

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  #if req.reference.ids and req.reference.ids != (0,):
  #  ref_ids = req.reference.ids
  #elif req.reference.class_types:
  #  ref_ids = any_obj_types_ids( ref_obj, req.reference.class_types ).all()
  #else:
  #  ref_ids = any_obj_ids( ref_obj ).all()
  #if req.target.ids and req.target.ids != (0,):
  #  tar_ids = req.target.ids
  #elif req.target.class_types:
  #  tar_ids = any_obj_types_ids( tar_obj, req.target.class_types ).all()
  #else:
  #  tar_ids = any_obj_ids( tar_obj ).all()


  ref_ids =constrain_obj_ids(ref_obj, req.reference).all()
  tar_ids =constrain_obj_ids(tar_obj, req.target).all()

  if ref_status == "2D":
    operator = ST_Overlaps(tar_geo.geometry, ref_geo.geometry)
  else:
    operator = SFCGAL_Intersects3D(ref_geo.geometry, tar_geo.geometry)

  retrieval =  ( rospy.Time.now() - then ).to_sec()

  if req.reference.geometry_type == "Body" and req.target.geometry_type == "Body":
    ids = db().query( ref_obj.id, tar_obj.id ).\
               filter( ref_obj.id.in_( ref_ids ),
                       ref_obj.absolute_description_id == ref_geo.geometry_desc,
                       ref_geo.type == req.reference.geometry_type,
                       tar_obj.id.in_( tar_ids ),
                       tar_obj.absolute_description_id == tar_geo.geometry_desc,
                       tar_geo.type == req.target.geometry_type,
                       ref_obj.id != tar_obj.id,
                       operator).order_by(ref_obj.id).all()
  else:
    #print  'ref sql', constrain_obj_ids(ref_obj, req.reference)
    #print  'ref geo', constrain_obj_ids(ref_obj, req.reference).all()
    res.refs =  len(ref_ids)
    res.tars =  len(tar_ids)
    res.num_tests =  len(ref_ids) * len(tar_ids)
    #test = db().query( ref_obj.id, tar_obj.id ).\
      #filter(
              #ref_geo.id.in_( constrain_geo_ids(ref_obj, ref_geo, req.reference).correlate(ref_obj.absolute_description_id == ref_geo.geometry_desc) ),
              #tar_geo.id.in_( constrain_geo_ids(tar_obj, tar_geo, req.target).correlate(tar_obj.absolute_description_id == tar_geo.geometry_desc) ),
              #ref_obj.id != tar_obj.id,
              #operator
            #).all()

    #print  'ref', constrain_obj_ids(ref_obj, req.reference).all()

    #print  'tar', constrain_obj_ids(tar_obj, req.target).all()

    #print test

    ids = db().query( ref_obj.id, tar_obj.id ).\
      filter(  #or_( ref_obj.id.in_( ref_ids ), ref_obj.id.in_( req.reference.ids ))
                   ref_obj.id.in_( constrain_obj_ids(ref_obj, req.reference) ),
                   ref_obj.absolute_description_id == ref_geo.abstraction_desc,
                   ref_geo.type == req.reference.geometry_type,

                   tar_obj.id.in_( constrain_obj_ids(tar_obj, req.target) ),
                   tar_obj.absolute_description_id == tar_geo.abstraction_desc,
                   tar_geo.type == req.target.geometry_type,
                   ref_obj.id != tar_obj.id,
                   operator).order_by(ref_obj.id).all()

  testing = ( rospy.Time.now() - then ).to_sec() - retrieval

  if ids:
    old_i = None
    one_to_many = OneToMany()
    one_to_many.reference_id = ids[0][0]
    for i, d in ids:
      if one_to_many.reference_id != i:
        res.results.append(one_to_many)
        one_to_many = OneToMany()
        one_to_many.reference_id = i
      one_to_many.target_ids.append(d)
      pair = ObjectPair()
      pair.reference_id = i
      pair.target_id = d
      pair.relations.append("intersects")
      res.pairs.append(pair)
    res.results.append(one_to_many)
  packaging =  ( rospy.Time.now() - then ).to_sec() - retrieval - testing
  #rospy.loginfo( "ObjectsIntersectObjects %f:" % ( rospy.Time.now() - then ).to_sec() )
  #rospy.loginfo( "  Retrieving     %f:" % retrieval)
  #rospy.loginfo( "  Testing        %f:" % testing)
  #rospy.loginfo( "  Packaging      %f:" % packaging )

  #print '#pairs', len(res.pairs)

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
  return res

###
### DIRECTIONAL OPERATORS
###

def get_directional_relations3d( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_directional_relations3d" )
  res = GetDirectionalRelations2DResponse()

  ref_obj = aliased( ObjectInstance )
  ref_geo = aliased( GeometryModel )
  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  if False: #req.geometry_type not in ["Position2D", "AxisAligned2D", "FootprintBox",  "FootprintHull"]:
    rospy.logerr("SEMAP DB SRVs: get_directional_relations3d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
    rospy.loginfo("SEMAP DB SRVs: get_directional_relation32d tries to find em")

  relations = {}
  relations["infront-of"] = ["FrontProjection" , "FrontHalfspace"]
  relations["behind-of"]  = ["BackProjection", "BackHalfspace"]
  relations["left-of"]    = ["LeftProjection", "LeftHalfspace"]
  relations["right-of"]   = ["RightProjection", "RightHalfspace"]
  relations["above-of"]   = ["TopProjection", "TopHalfspace"]
  relations["below-of"]   = ["BotProjection", "BotHalfspace"]

  for relation in relations.keys():

    for geometry_type in relations[relation]:

      results = db().query( SFCGAL_Contains3D( ref_geo.geometry, tar_geo.geometry), SFCGAL_Intersects3D( ref_geo.geometry, tar_geo.geometry) ).\
            filter(ref_obj.id == req.reference_id,
                   ref_obj.absolute_description_id == ref_geo.abstraction_desc, ref_geo.type == geometry_type, \
                   tar_obj.id ==  req.target_id,
                   tar_obj.absolute_description_id == tar_geo.abstraction_desc, tar_geo.type == "BoundingBox" ).all()

      for strict, relaxed in results:
        if "Projection" in geometry_type:
          if strict:
            res.relations.append("ps_strict-" + relation)
          if relaxed:
            res.relations.append("ps_relaxed-" + relation)
        elif "Halfspace" in geometry_type:
          if strict:
            res.relations.append("hs_strict-" + relation)
          if relaxed:
            res.relations.append("hs_relaxed-" + relation)

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
    rospy.logerr("SEMAP DB SRVs: get_directional_relations2d was called with %s which is not a valid 2D geometry type" % req.geometry_type)
  else:
      rospy.loginfo("SEMAP DB SRVs: get_directional_relations2d tries to find em")

      obj_ids1 = any_obj_types_ids(obj1, ["ConferenceChair"])
      obj_ids2 = any_obj_types_ids(obj2, ["ConferenceChair"])

###
### OTHER OPERATORS
###

'''
def get_objects_on_object( req ):

  rospy.logdebug( "SEMAP DB SRVs: get_objects_on" )

  res = GetObjectsOnObjectResponse()

  ref_obj = aliased( ObjectInstance )
  ref_geo = aliased( GeometryModel )
  ref_top = aliased( GeometryModel )

  tar_obj = aliased( ObjectInstance )
  tar_geo = aliased( GeometryModel )

  if req.target_object_types:
    tar_obj_ids = any_obj_types_ids(tar_obj, req.target_object_types).all()
  else:
    tar_obj_ids = any_obj_ids(tar_obj).all()

  pairs = db().query( ref_obj.id, tar_obj.id ).\
              filter( ref_obj.id = req.reference_object_id,
                     ref_obj.absolute_description_id == ref_geo.abstraction_desc,
                     ref_geo.type == "BoundingBox", \
                     ref_obj.id.in_( ref_obj_ids ),
                     ref_obj.absolute_description_id == ref_top.abstraction_desc,
                     ref_top.type == "TopProjection", \
                     tar_obj.id.in_( tar_obj_ids ),
                     tar_obj.absolute_description_id == tar_geo.abstraction_desc, tar_geo.type == "BoundingBox", \
                     SFCGAL_Contains3D( ref_top.geometry, tar_geo.geometry),
                     SFCGAL_Distance3D( ref_geo.geometry, tar_geo.geometry) < req.threshold).all()

  for ref_id, tar_id in pairs:
    pair = ObjectPair()
    pair.reference_id = ref_id
    pair.target_id = tar_id
    pair.relations.append("supports")
    res.pairs.append(pair)
    pair = ObjectPair()
    pair.reference_id = tar_id
    pair.target_id = ref_id
    pair.relations.append("is-on")
    res.pairs.append(pair)

  return res
'''

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
