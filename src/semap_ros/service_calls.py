#!/usr/bin/env python

import roslib; roslib.load_manifest('semap_ros')
import rospy

from semap_ros.srv import *

def call_add_root_frame(frame):
  try:
    rospy.wait_for_service('add_root_frame')
    request = AddRootFrameRequest()
    request.frame = frame
    call = rospy.ServiceProxy('add_root_frame', AddRootFrame)
    response = call(request)
    print 'AddRootFrame service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "AddRootFrame service call failed: %s" % e

###
### Spatial Relations
###

def call_get_objects_within_polygon2d(object_types, geometry_type, polygon, fully_within = False):
  try:
    rospy.wait_for_service('get_objects_within_polygon2d')
    call = rospy.ServiceProxy('get_objects_within_polygon2d', GetObjectsWithinPolygon2D)
    request = GetObjectsWithinPolygon2DRequest()
    request.object_types = object_types
    request.geometry_type = geometry_type
    request.polygon = polygon
    request.fully_within = fully_within
    response = call(request)
    rospy.loginfo('GetObjectsWithinPolygon2D services call succeeded!')
    print response
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsWithinPolygon2D service call failed: %s" % e

def call_get_objects_within_range(reference_point, target_object_types, target_object_geometry_type, distance, fully_within = False):
  try:
    rospy.wait_for_service('get_objects_within_range')
    call = rospy.ServiceProxy('get_objects_within_range', GetObjectsWithinRange)
    request = GetObjectsWithinRangeRequest()
    request.reference_point = reference_point
    request.target_object_types = target_object_types
    request.target_object_geometry_type = target_object_geometry_type
    request.distance = distance
    request.fully_within = fully_within
    print 'PRECALL'
    response = call(request)
    rospy.loginfo('GetObjectsWithinRange services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsWithinRange service call failed: %s" % e

def call_get_distance_between_objects3d(reference_object_types, reference_object_geometry_type, target_object_types, target_object_geometry_type, min_range = 0.0, max_range = 0.0, sort_descending = False, max_distance = False, return_points = False):
  try:
    rospy.wait_for_service('get_distance_between_objects3d')
    call = rospy.ServiceProxy('get_distance_between_objects3d', GetDistanceBetweenObjects3D)
    request = GetDistanceBetweenObjects3DRequest()
    request.reference_object_types = reference_object_types
    request.reference_object_geometry_type = reference_object_geometry_type
    request.target_object_types = target_object_types
    request.target_object_geometry_type = target_object_geometry_type
    request.min_range = min_range
    request.max_range = max_range
    request.sort_descending = sort_descending
    request.max_distance = max_distance
    request.return_points = return_points
    response = call(request)
    rospy.loginfo('GetDistanceBetweenObjects3D services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetDistanceBetweenObjects3D service call failed: %s" % e


def call_get_directional_relations2d(refrence_id, target_id, geometry_type):
  try:
    rospy.wait_for_service('get_directional_relations2d')
    call = rospy.ServiceProxy('get_directional_relations2d', GetDirectionalRelations2D)
    request = GetDirectionalRelations2DRequest()
    request.reference_id = reference_id
    request.target_id = target_id
    request.geometry_type = geometry_type
    response = call(request)
    rospy.loginfo('GetDirectionalRelations2D services call succeeded!')
    print response
    return response
  except rospy.ServiceException as e:
    return None, "GetDirectionalRelations2D service call failed: %s" % e

def call_test_containment_relations3d(refrence_id, target_id, geometry_type):
  try:
    rospy.wait_for_service('test_containment_relations3d')
    call = rospy.ServiceProxy('test_containment_relations3d', GetDirectionalRelations2D)
    request = GetDirectionalRelations2DRequest()
    request.reference_id = reference_id
    request.target_id = target_id
    request.geometry_type = geometry_type
    response = call(request)
    rospy.loginfo('TestContainmentRelations3d services call succeeded!')
    print response
    return response
  except rospy.ServiceException as e:
    return None, "TestContainmentRelations3d service call failed: %s" % e


###

###
### Tests
###

def call_unary_relation_test(id, type, relations):
  try:
    rospy.wait_for_service('unary_relation_test')
    call = rospy.ServiceProxy('unary_relation_test', UnaryRelationTest)
    request = UnaryRelationTestRequest()
    request.id = id
    request.type = type
    request.relations = relations
    response = call(request)
    print 'UnaryRelationTest services call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "UnaryRelationTest service call failed: %s" % e

def call_binary_relation_test(id1, type1, id2, type2, relations):
  try:
    rospy.wait_for_service('binary_relation_test')
    call = rospy.ServiceProxy('binary_relation_test', BinaryRelationTest)
    request = BinaryRelationTestRequest()
    request.id1 = id1
    request.type1 = type1
    request.id2 = id2
    request.type2 = type2
    request.relations = relations
    response = call(request)
    print 'BinaryRelationTest services call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "BinaryRelationTest service call failed: %s" % e
