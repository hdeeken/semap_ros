#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest('semap_ros')

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

##
## manual queries
##

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
    response = call(request)
    rospy.logdebug('GetObjectsWithinRange services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsWithinRange service call failed: %s" % e

def call_get_objects_within_area(reference_polygon, target_object_types, target_object_geometry_type, fully_within = False):
  try:
    rospy.wait_for_service('get_objects_within_area')
    call = rospy.ServiceProxy('get_objects_within_area', GetObjectsWithinArea)
    request = GetObjectsWithinAreaRequest()
    request.reference_polygon = reference_polygon
    request.target_object_types = target_object_types
    request.target_object_geometry_type = target_object_geometry_type
    request.fully_within = fully_within
    response = call(request)
    rospy.logdebug('GetObjectsWithinArea services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsWithinArea service call failed: %s" % e

def call_get_objects_within_volume(reference_mesh, target_object_types, target_object_geometry_type, fully_within = False):
  try:
    rospy.wait_for_service('get_objects_within_volume')
    call = rospy.ServiceProxy('get_objects_within_volume', GetObjectsWithinVolume)
    request = GetObjectsWithinVolumeRequest()
    request.reference_mesh = reference_mesh
    request.target_object_types = target_object_types
    request.target_object_geometry_type = target_object_geometry_type
    request.fully_within = fully_within
    response = call(request)
    rospy.logdebug('GetObjectsWithinVolume services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsWithinVolume service call failed: %s" % e

#
# one to many queries
#

def call_get_directional_relations2d(refrence_id, target_id, geometry_type):
  try:
    rospy.wait_for_service('get_directional_relations2d')
    call = rospy.ServiceProxy('get_directional_relations2d', GetDirectionalRelations2D)
    request = GetDirectionalRelations2DRequest()
    request.reference_id = reference_id
    request.target_id = target_id
    request.geometry_type = geometry_type
    response = call(request)
    rospy.logdebug('GetDirectionalRelations2D services call succeeded!')
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
    rospy.logdebug('TestContainmentRelations3d services call succeeded!')
    print response
    return response
  except rospy.ServiceException as e:
    return None, "TestContainmentRelations3d service call failed: %s" % e

#
# one to many
#

def call_get_objects_on_object(reference_object_id, target_object_types, threshold = 0.05):
  try:
    rospy.wait_for_service('get_objects_on_object')
    call = rospy.ServiceProxy('get_objects_on_object', GetObjectsOnObject)
    request = GetObjectsOnObject()
    request.reference_object_types = reference_object_types
    request.target_object_types = target_object_types
    request.threshold = threshold
    response = call(request)
    rospy.logdebug('GetObjectsOnObject services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsOnObject service call failed: %s" % e

def call_get_objects_within_object(reference_object_id, target_object_types, fully_within = True):
  try:
    rospy.wait_for_service('get_objects_within_object')
    call = rospy.ServiceProxy('get_objects_within_object', GetObjectsWithinObject)
    request = GetObjectsWithinObjectRequest()
    request.reference_object_id = reference_object_id
    request.target_object_types = target_object_types
    request.fully_within = fully_within
    response = call(request)
    rospy.logdebug('GetObjectsWithinObject services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsWithinObject service call failed: %s" % e

#
# many to many
#

def call_get_objects_intersect_objects(reference, target):
  try:
    rospy.wait_for_service('get_objects_intersect_objects')
    call = rospy.ServiceProxy('get_objects_intersect_objects', GetObjectsIntersectObjects)
    request = GetObjectsIntersectObjectsRequest()
    request.reference = reference
    request.target = target
    response = call(request)
    rospy.logdebug('GetObjectsIntersectObjects services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsIntersectObjects service call failed: %s" % e

def call_get_objects_in_objects(reference, target, fully_within = True):
  try:
    rospy.wait_for_service('get_objects_in_objects')
    call = rospy.ServiceProxy('get_objects_in_objects', GetObjectsInObjects)
    request = GetObjectsInObjectsRequest()
    request.reference = reference
    request.target = target
    request.fully_within = fully_within
    response = call(request)
    rospy.logdebug('GetObjectsInObjects services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsInObjects service call failed: %s" % e

def call_get_objects_on_objects(reference, target, threshold = 0.05):
  try:
    rospy.wait_for_service('get_objects_on_objects')
    call = rospy.ServiceProxy('get_objects_on_objects', GetObjectsOnObjects)
    request = GetObjectsOnObjectsRequest()
    request.reference = reference
    request.target = target
    request.threshold = threshold
    response = call(request)
    rospy.logdebug('GetObjectsOnObjects services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectsOnObjects service call failed: %s" % e


def call_get_distance_between_objects(reference_object_types, reference_object_geometry_type, target_object_types, target_object_geometry_type, min_range = 0.0, max_range = 0.0, sort_descending = False, max_distance = False, return_points = False):
  try:
    rospy.wait_for_service('get_distance_between_objects')
    call = rospy.ServiceProxy('get_distance_between_objects', GetDistanceBetweenObjects)
    request = GetDistanceBetweenObjectsRequest()
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
    rospy.logdebug('GetDistanceBetweenObjects services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetDistanceBetweenObjects3D service call failed: %s" % e

###
### UTIL
###

def call_extrude_polygon(polygon, x, y, z):
  try:
    rospy.wait_for_service('extrude_polygon')
    call = rospy.ServiceProxy('extrude_polygon', ExtrudePolygon)
    request = ExtrudePolygonRequest()
    request.polygon = polygon
    request.vector.x = x
    request.vector.y = y
    request.vector.z = z
    response = call(request)
    print 'ExtrudePolygon services call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "ExtrudePolygon service call failed: %s" % e

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
