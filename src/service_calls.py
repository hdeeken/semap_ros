#!/usr/bin/env python

'''
SpatialDB Service Calls
'''

import roslib; roslib.load_manifest('spatial_db')
import rospy

from spatial_db_ros.srv import *

def add_root_frame(frame):
  try:
    rospy.wait_for_service('add_root_frame')
    request = AddRootFrameRequest()
    request.frame = frame
    srv_call = rospy.ServiceProxy('add_root_frame', AddRootFrame)
    response = srv_call(request)
    print 'AddRootFrame service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "AddRootFrame service call failed: %s" % e


def add_object_descriptions(descriptions):
  try:
    rospy.wait_for_service('add_object_descriptions')
    request = AddObjectDescriptionsRequest()
    request.descriptions = descriptions
    add_object_descriptions_call = rospy.ServiceProxy('add_object_descriptions', AddObjectDescriptions)
    response = add_object_descriptions_call(request)
    print 'AddObjectDescriptions service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "AddObjectDescriptions service call failed: %s" % e

def get_object_descriptions(ids):
  try:
    rospy.wait_for_service('get_object_descriptions')
    get_object_descriptions_call = rospy.ServiceProxy('get_object_descriptions', GetObjectDescriptions)
    request = GetObjectDescriptionsRequest()
    request.ids = ids
    response = get_object_descriptions_call(request)
    print 'GetObjectDescriptions service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectDescriptions service call failed: %s" % e

def get_object_description_id(type):
  try:
    rospy.wait_for_service('get_object_description_id')
    call = rospy.ServiceProxy('get_object_description_id', GetObjectDescriptionID)
    request = GetObjectDescriptionIDRequest()
    request.type = type
    response = call(request)
    print 'GetObjectDescriptionID service call succeeded!'
    print 'id of', type, 'is', response.id
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectDescriptionID service call failed: %s" % e

def get_all_object_descriptions():
  try:
    rospy.wait_for_service('get_all_object_descriptions')
    get_all_object_descriptions_call = rospy.ServiceProxy('get_all_object_descriptions', GetAllObjectDescriptions)
    request = GetAllObjectDescriptionsRequest()
    response = get_all_object_descriptions_call(request)
    print 'GetAllObjectDescriptions service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetAllObjectDescriptions service call failed: %s" % e

#inst
def add_object_instances(objects):
  try:
    rospy.wait_for_service('add_object_instances')
    request = AddObjectInstancesRequest()
    request.objects = objects
    add_object_instances_call = rospy.ServiceProxy('add_object_instances', AddObjectInstances)
    response = add_object_instances_call(request)
    print 'AddObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "AddObjectInstances service call failed: %s" % e

def get_object_instances(ids):
  try:
    rospy.wait_for_service('get_all_object_instances')
    get_object_instances_call = rospy.ServiceProxy('get_object_instances', GetObjectInstances)
    request = GetObjectInstancesRequest()
    request.ids = ids
    response = get_object_instances_call(request)
    print 'GetObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectInstances service call failed: %s" % e

def get_all_object_instances():
  try:
    rospy.wait_for_service('get_all_object_instances')
    get_all_object_instances_call = rospy.ServiceProxy('get_all_object_instances', GetAllObjectInstances)
    request = GetAllObjectInstancesRequest()
    response = get_all_object_instances_call(request)
    print 'GetAllObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetAllObjectInstances service call failed: %s" % e

def activate_all_object_instances():
  try:
    rospy.wait_for_service('activate_all_object_instances')
    activate_all_object_instances_call = rospy.ServiceProxy('activate_all_object_instances', ActivateAllObjectInstances)
    request = ActivateAllObjectInstancesRequest()
    response = activate_all_object_instances_call(request)
    print 'ActivateAllObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "ActivateAllObjectInstances service call failed: %s" % e

def update_transform(id, pose):
  try:
    rospy.wait_for_service('update_transform')
    update_transform_call = rospy.ServiceProxy('update_transform', UpdateTransform)
    request = UpdateTransformRequest()
    request.id = id
    request.pose.position.x = pose[0][0]
    request.pose.position.y = pose[0][1]
    request.pose.position.z = pose[0][2]
    request.pose.orientation.x = pose[1][0]
    request.pose.orientation.y = pose[1][1]
    request.pose.orientation.z = pose[1][2]
    request.pose.orientation.w = pose[1][3]
    response = update_transform_call(request)
    print 'UpdateTransform service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "UpdateTransform service call failed: %s" % e

def set_transform(id, transform):
  try:
    rospy.wait_for_service('set_transform')
    update_transform_call = rospy.ServiceProxy('set_transform', SetTransform)
    request = SetTransformRequest()
    request.id = id
    request.pose.position.x = transform[0][0]
    request.pose.position.y = transform[0][1]
    request.pose.position.z = transform[0][2]
    request.pose.orientation.x = transform[1][0]
    request.pose.orientation.y = transform[1][1]
    request.pose.orientation.z = transform[1][2]
    request.pose.orientation.w = transform[1][3]
    response = update_transform_call(request)
    print 'SetTransform service call succeeded!'
    return True
  except rospy.ServiceException as e:
    return None, "SetTransform service call failed: %s" % e

def change_frame(id, frame, keep_transform):
  try:
    rospy.wait_for_service('change_frame')
    change_frame = rospy.ServiceProxy('change_frame', ChangeFrame)
    request = ChangeFrameRequest()
    request.id = id
    request.frame = frame
    request.keep_transform = keep_transform
    response = change_frame(request)
    print 'ChangeFrame service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "ChangeFrame service call failed: %s" % e
