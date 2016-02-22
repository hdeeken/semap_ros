#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'semap_ros' )

from copy import deepcopy

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
from semap_ros.spatial_relations import *
from semap_ros.subqueries import *
from semap_ros.instance_srv_calls import *

from semap_msgs.msg import ObjectPair

def bind_objects_on_objects(req):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_object" )

  call_get_objects_in_objects(req.reference_object_types, req.target_types, req.fully_within)

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

def unbind_objects_on_objects(req):
  rospy.loginfo( "SEMAP DB SRVs: get_objects_within_object" )

  call_get_objects_in_objects(req.reference_object_types, req.target_types, req.fully_within)

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

def structure_by_support( req ):
  rospy.loginfo( "SEMAP DB SRVs: structure_by_support" )
  res = GetObjectsOnObjectsResponse()

  then = rospy.Time.now()

  req_2d = GetObjectsInObjectsRequest()
  req_2d.reference =  deepcopy(req.reference)
  req_2d.reference.geometry_type = "FootprintHull"
  req_2d.target =  deepcopy(req.target)
  req_2d.target.geometry_type = "FootprintHull"
  req_2d.fully_within = True
  res_2d = get_objects_in_objects(req_2d)

  pre_query =  ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "2D Pre-Query %f:" % pre_query )

  #print req
  req_3d = deepcopy(req)
  req_3d.reference.class_types = []
  req_3d.reference.superclass_types = []
  req_3d.target.class_types = []
  req_3d.target.superclass_types = []

  intermediate = 0.0
  for result in res_2d.results:
    req_3d.reference.ids = [result.reference_id]
    req_3d.target.ids = result.target_ids
    intermediate_then = rospy.Time.now()
    res_3d = get_objects_on_objects(req_3d)
    intermediate =  ( rospy.Time.now() - intermediate_then ).to_sec()
    res.pairs += res_3d.pairs
    res.results += res_3d.results
    res.results += res_3d.results

  validation =  ( rospy.Time.now() - then ).to_sec() - pre_query
  rospy.loginfo( "3D Validation %f:" % validation )
  total = ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "2D/3D Total %f:" % total )

  #then = rospy.Time.now()
  #res = get_objects_on_objects(req)
  #direct =  ( rospy.Time.now() - then ).to_sec()
  #rospy.loginfo( "3D Direct Query %f:" % direct )
  #rospy.loginfo("prepared query takes only %f of time that the direct needs that %f improvement" % (total/direct, direct/total) )

  for pair in res.pairs:
    if pair.reference_id != pair.target_id:
      print 'bind', pair.target_id, 'to', pair.reference_id
      call_change_frame(pair.target_id, 'object'+str(pair.reference_id), False)

  total = ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "SRV Bound Total %f:" % total )

  return res


def structure_by_containment( req ):
  rospy.loginfo( "SEMAP DB SRVs: structure_by_containment" )
  res = GetObjectsInObjectsResponse()

  then = rospy.Time.now()

  req_2d = deepcopy(req)
  req_2d.reference.geometry_type = "FootprintBox"
  req_2d.target.geometry_type = "Position2D"
  res_2d = get_objects_in_objects(req_2d)

  pre_query =  ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "2D Pre-Query %f:" % pre_query )

  #print req
  req_3d = deepcopy(req)
  req_3d.reference.class_types = []
  req_3d.reference.superclass_types = []
  req_3d.target.class_types = []
  req_3d.target.superclass_types = []

  intermediate = 0.0
  for result in res_2d.results:
    rospy.loginfo( "3D Intermediate %f:" % intermediate)
    req_3d.reference.ids = [result.reference_id]
    req_3d.target.ids = result.target_ids
    #print '2D SET'
    #print result
    #print req_3d
    intermediate_then = rospy.Time.now()
    res_3d = get_objects_in_objects(req_3d)
    intermediate =  ( rospy.Time.now() - intermediate_then ).to_sec()
    #print '3D SET'
    #print res_3d.pairs
    res.pairs += res_3d.pairs
    res.results += res_3d.results

  validation =  ( rospy.Time.now() - then ).to_sec() - pre_query
  rospy.loginfo( "3D Validation %f:" % validation )
  total = ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "2D/3D Total %f:" % total )

  #then = rospy.Time.now()
  #res = get_objects_in_objects(req)
  #direct =  ( rospy.Time.now() - then ).to_sec()
  #rospy.loginfo( "3D Direct Query %f:" % direct )
  #rospy.loginfo("prepared query takes only %f of time that the direct needs that %f improvement" % (total/direct, direct/total) )

  for pair in res.pairs:
    if pair.reference_id != pair.target_id:
      #print 'bind', pair.target_id, 'to', pair.reference_id
      call_change_frame(pair.target_id, 'object'+str(pair.reference_id), False)

  total = ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "SRV Bound Total %f:" % total )

  return res

def unstructure_by_containment( req ):
  rospy.loginfo( "SEMAP DB SRVs: structure_by_containment" )
  res = GetObjectsInObjectsResponse()

  then = rospy.Time.now()

  req_2d = deepcopy(req)
  req_2d.reference.geometry_type = "FootprintBox"
  req_2d.target.geometry_type = "Position2D"
  res_2d = get_objects_in_objects(req_2d)

  pre_query =  ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "2D Pre-Query %f:" % pre_query )

  #print req
  req_3d = deepcopy(req)
  req_3d.reference.class_types = []
  req_3d.reference.superclass_types = []
  req_3d.target.class_types = []
  req_3d.target.superclass_types = []

  before = ( rospy.Time.now() - then ).to_sec()
  for result in res_2d.results:
    req_3d.reference.ids = [result.reference_id]
    req_3d.target.ids = result.target_ids
    #print '2D SET'
    #print result
    #print req_3d
    before = ( rospy.Time.now() - then ).to_sec()
    res_3d = get_objects_in_objects(req_3d)
    intermediate = ( rospy.Time.now() - then ).to_sec() - before
    rospy.loginfo( "3D Intermediate %f:" % intermediate)
    #print '3D SET'
    #print res_3d.pairs
    res.pairs += res_3d.pairs
    res.results += res_3d.results

  validation =  ( rospy.Time.now() - then ).to_sec() - pre_query
  rospy.loginfo( "3D Validation %f:" % validation )
  total = ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "2D/3D Total %f:" % total )

  then = rospy.Time.now()
  res = get_objects_in_objects(req)
  direct =  ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "3D Direct Query %f:" % direct )
  rospy.loginfo("prepared query takes only %f of time that the direct needs that %f improvement" % (total/direct, direct/total) )

  for pair in res.pairs:
    if pair.reference_id != pair.target_id:
      print 'bind', pair.target_id, 'to world'
      call_change_frame(pair.target_id, 'world', False)

  total = ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "SRV Unbound Total %f:" % total )

  return res

def evaluate_intersection( req ):

  res = GetObjectsIntersectObjectsResponse()
  '''
  then = rospy.Time.now()
  req_2d = deepcopy(req)
  req_2d.reference.geometry_type = "FootprintBox"
  req_2d.target.geometry_type = "FootprintBox"
  res_2d = get_objects_intersect_objects(req_2d)
  pre_query =  ( rospy.Time.now() - then ).to_sec()

  #print req
  req_3d = deepcopy(req)
  req_3d.reference.class_types = []
  req_3d.reference.superclass_types = []
  req_3d.target.class_types = []
  req_3d.target.superclass_types = []

  num_tests = 0
  before = ( rospy.Time.now() - then ).to_sec()
  for result in res_2d.results:
    req_3d.reference.ids = [result.reference_id]
    req_3d.target.ids = result.target_ids
    num_tests += len(result.target_ids)
    res_3d = get_objects_intersect_objects(req_3d)
    intermediate = ( rospy.Time.now() - then ).to_sec() - before
    #rospy.loginfo( "3D Intermediate %f:" % intermediate)
    res.pairs += res_3d.pairs
    res.results += res_3d.results

  validation =  ( rospy.Time.now() - then ).to_sec() - pre_query
  total = ( rospy.Time.now() - then ).to_sec()

  print "SEMAP DB SRVs: evaluate_intersection"
  print "2D Pre-Query :",  pre_query
  print "3D Validation:", validation
  print "num tests %d :", num_tests
  print "2D/3D Total  :", total
  '''
  then = rospy.Time.now()
  res = get_objects_intersect_objects(req)
  direct_total =  ( rospy.Time.now() - then ).to_sec()
  #direct = 3380.266240

  print "Direct Query:"
  print "  Ref:", req.reference.superclass_types
  print "  #num:", res.refs
  print "  #geo:", req.reference.geometry_type
  print "  Tar:",  req.target.superclass_types
  print "  #num:", res.tars
  print "  #geo:", req.target.geometry_type
  print "  Tests:", res.num_tests
  print "  Pairs:", len(res.pairs)
  print "  Time / Tests", "{:10.8f}".format(direct_total / res.num_tests)
  print "3D Direct Query:", direct_total

  #print "prepared query takes only %f of time that the direct needs that %f improvement" % (total/direct, direct/total)

  total = ( rospy.Time.now() - then ).to_sec()
  rospy.loginfo( "SRV Intersects Total %f:" % total )

  return res

def evaluate_containment( req ):
  rospy.loginfo( "SEMAP DB SRVs: evaluate_containment" )
  res = GetObjectsInObjectsResponse()

  then = rospy.Time.now()

  req_2d = deepcopy(req)
  req_2d.reference.geometry_type = "FootprintBox"
  req_2d.target.geometry_type = "FootprintBox"
  res_2d = get_objects_in_objects(req_2d)

  pre_query =  ( rospy.Time.now() - then ).to_sec()
  inital_num_tests = res_2d.num_tests
  inital_num_refs = res_2d.refs
  inital_num_tars =  res_2d.tars

  #print req
  req_3d = deepcopy(req)
  req_3d.reference.class_types = []
  req_3d.reference.superclass_types = []
  req_3d.target.class_types = []
  req_3d.target.superclass_types = []

  for result in res_2d.results:
    req_3d.reference.ids = [result.reference_id]
    req_3d.target.ids = result.target_ids
    print 'ROOM ID:', result.reference_id
    res_3d = get_objects_in_objects(req_3d)
    res.pairs += res_3d.pairs
    res.results += res_3d.results
    res.num_tests += res_3d.num_tests

  validation =  ( rospy.Time.now() - then ).to_sec() - pre_query
  direct_total = ( rospy.Time.now() - then ).to_sec()

  print "Cascaded Query:"
  print "  Ref:", req.reference.superclass_types
  print "  #num:", inital_num_refs
  print "  #geo:", req.reference.geometry_type
  print "  Tar:",  req.target.superclass_types
  print "  #num:", inital_num_tars
  print "  #geo:", req.target.geometry_type
  print "  Tests:", inital_num_tests
  print "  Tests:", res.num_tests
  print "  Pairs:", len(res.pairs)
  print "  Time / Tests", "{:10.6f}".format(direct_total / res.num_tests)

  print "2D Pre-Query :",  pre_query
  print "3D Validation:", validation
  print "2D/3D Total  :", direct_total
  '''
  then = rospy.Time.now()
  res = get_objects_in_objects(req)
  direct_total =  ( rospy.Time.now() - then ).to_sec()
  print "Direct Query:"
  print "  Ref:", req.reference.superclass_types
  print "  #num:", res.refs
  print "  #geo:", req.reference.geometry_type
  print "  Tar:",  req.target.superclass_types
  print "  #num:", res.tars
  print "  #geo:", req.target.geometry_type
  print "  Tests:", res.num_tests
  print "  Pairs:", len(res.pairs)
  print "  Time / Tests", "{:10.6f}".format(direct_total / res.num_tests)
  print "Direct Query:", direct_total

  print "Containtment &", req.reference.superclass_types, "&", req.reference.geometry_type, "&",res.refs, "&", req.target.superclass_types, "&", req.target.geometry_type, "&",res.tars, "&", res.num_tests, "&", "{:10.6f}".format(direct_total / res.num_tests), "&", direct_total, "&", len(res.pairs)
  #rospy.loginfo("prepared query takes only %f of time that the direct needs that %f improvement" % (total/direct, direct/total) )
  '''
  return res
