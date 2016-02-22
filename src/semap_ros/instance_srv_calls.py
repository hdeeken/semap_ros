#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest('semap_ros')

from semap_ros.srv import *

def call_add_object_instances(objects):
  try:
    rospy.wait_for_service('add_object_instances')
    request = AddObjectInstancesRequest()
    request.objects = objects
    add_object_instances_call = rospy.ServiceProxy('add_object_instances', AddObjectInstances)
    response = add_object_instances_call(request)
    rospy.logdebug('AddObjectInstances service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "AddObjectInstances service call failed: %s" % e

def call_rename_object_instance(id, alias):
  try:
    rospy.wait_for_service('rename_object_instance')
    request = RenameObjectInstanceRequest()
    request.id = id
    request.alias = alias
    call = rospy.ServiceProxy('rename_object_instance', RenameObjectInstance)
    response = call(request)
    rospy.logdebug('RenameObjectInstance service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "RenameObjectInstance service call failed: %s" % e

def call_switch_object_descriptions(obj_ids, desc_id):
  try:
    rospy.wait_for_service('switch_object_descriptions')
    request = SwitchObjectDescriptionsRequest()
    request.obj_ids = obj_ids
    request.desc_id = desc_id
    call = rospy.ServiceProxy('switch_object_descriptions', SwitchObjectDescriptions)
    response = call(request)
    rospy.logdebug('SwitchObjectDescriptions service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "SwitchObjectDescriptions service call failed: %s" % e

def call_delete_object_instances(ids, keep_children = True, child_frame = "", keep_transform = False):
  try:
    rospy.wait_for_service('delete_object_instances')
    request = DeleteObjectInstancesRequest()
    request.ids = ids
    request.keep_children = keep_children
    request.child_frame = child_frame
    request.keep_transform = keep_transform
    call = rospy.ServiceProxy('delete_object_instances', DeleteObjectInstances)
    response = call(request)
    rospy.logdebug('DeleteObjectInstances service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "DeleteObjectInstances service call failed: %s" % e

#inst

def call_get_object_instances(ids):
  try:
    rospy.wait_for_service('get_all_object_instances')
    rospy.loginfo('GetObjectInstances service call...')
    call = rospy.ServiceProxy('get_object_instances', GetObjectInstances)
    request = GetObjectInstancesRequest()
    request.ids = ids
    response = call(request)
    rospy.loginfo('GetObjectInstances service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectInstances service call failed: %s" % e

def call_get_object_instances_list():
  try:
    rospy.wait_for_service('get_object_instances_list')
    call = rospy.ServiceProxy('get_object_instances_list', GetObjectInstancesList)
    request = GetObjectInstancesListRequest()
    response = call(request)
    rospy.logdebug('GetObjectInstancesList service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectInstances service call failed: %s" % e

def call_copy_object_instances(ids):
  try:
    rospy.wait_for_service('copy_object_instances')
    request = CopyObjectInstancesRequest()
    request.ids = ids
    call = rospy.ServiceProxy('copy_object_instances', CopyObjectInstances)
    response = call(request)
    rospy.logdebug('CopyObjectInstances service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "CopyObjectInstances service call failed: %s" % e

def call_get_all_object_instances():
  try:
    rospy.wait_for_service('get_all_object_instances')
    get_all_object_instances_call = rospy.ServiceProxy('get_all_object_instances', GetAllObjectInstances)
    request = GetAllObjectInstancesRequest()
    response = get_all_object_instances_call(request)
    rospy.logdebug('GetAllObjectInstances service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetAllObjectInstances service call failed: %s" % e

#frame

def call_get_transform(source, target):
  try:
    rospy.wait_for_service('get_transform')
    call = rospy.ServiceProxy('get_transform', GetTransform)
    request = GetTransformRequest()
    request.source = source
    request.target = target
    response = call(request)
    rospy.logdebug('GetTransform services call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "GetTransform service call failed: %s" % e

def call_set_transform(id, pose):
  try:
    rospy.wait_for_service('set_transform')
    call = rospy.ServiceProxy('set_transform', SetTransform)
    request = SetTransformRequest()
    request.id = id
    request.pose = pose
    response = call(request)
    rospy.logdebug('SetTransform service call succeeded!')
    return True
  except rospy.ServiceException as e:
    return None, "SetTransform service call failed: %s" % e

def call_update_transform(id, pose):
  try:
    rospy.wait_for_service('update_transform')
    update_transform_call = rospy.ServiceProxy('update_transform', UpdateTransform)
    request = UpdateTransformRequest()
    request.id = id
    request.pose = pose
    response = update_transform_call(request)
    rospy.logdebug('UpdateTransform service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "UpdateTransform service call failed: %s" % e

def call_change_frame(id, frame, keep_transform):
  try:
    rospy.wait_for_service('change_frame')
    call = rospy.ServiceProxy('change_frame', ChangeFrame)
    request = ChangeFrameRequest()
    request.id = id
    request.frame = frame
    request.keep_transform = keep_transform
    response = call(request)
    rospy.logdebug('ChangeFrame service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "ChangeFrame service call failed: %s" % e

# absolute

def call_update_absolute_descriptions(ids):
  try:
    rospy.wait_for_service('update_absolute_descriptions')
    call = rospy.ServiceProxy('update_absolute_descriptions', UpdateAbsoluteDescriptions)
    request = UpdateAbsoluteDescriptionsRequest()
    request.ids = ids
    response = call(request)
    rospy.logdebug('UpdateAbsoluteDescriptions service call succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "UpdateAbsoluteDescriptions service call failed: %s" % e
