#!/usr/bin/env python

import roslib; roslib.load_manifest('semap_ros')
import rospy

from semap_ros.srv import *

def call_add_object_descriptions(descriptions):
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

def call_delete_object_descriptions(ids, keep_instances = True, new_desc_id = 0):
  try:
    rospy.wait_for_service('delete_object_descriptions')
    request = DeleteObjectDescriptionsRequest()
    request.ids = ids
    request.keep_instances = keep_instances
    request.new_desc_id = new_desc_id
    call = rospy.ServiceProxy('delete_object_descriptions', DeleteObjectDescriptions)
    response = call(request)
    print 'DeleteObjectDescriptions service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "DeleteObjectDescriptions service call failed: %s" % e

def call_copy_object_descriptions(ids):
  try:
    rospy.wait_for_service('copy_object_descriptions')
    request = CopyObjectDescriptionsRequest()
    request.ids = ids
    call = rospy.ServiceProxy('copy_object_descriptions', CopyObjectDescriptions)
    response = call(request)
    print 'CopyObjectDescriptions service call succeeded!'
  except rospy.ServiceException as e:
    return None, "CopyObjectDescriptions service call failed: %s" % e

def call_rename_object_description(id, type):
  try:
    rospy.wait_for_service('rename_object_description')
    request = RenameObjectDescriptionRequest()
    request.id = id
    request.type = type
    call = rospy.ServiceProxy('rename_object_description', RenameObjectDescription)
    response = call(request)
    print 'RenameObjectDescription service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "RenameObjectDescription service call failed: %s" % e

def call_get_object_descriptions(ids):
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

def call_get_object_description_id(type):
  try:
    rospy.wait_for_service('get_object_description_id')
    call = rospy.ServiceProxy('get_object_description_id', GetObjectDescriptionID)
    request = GetObjectDescriptionIDRequest()
    request.type = type
    response = call(request)
    print 'GetObjectDescriptionID service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectDescriptionID service call failed: %s" % e

def call_get_all_object_descriptions():
  try:
    rospy.wait_for_service('get_all_object_descriptions')
    get_all_object_descriptions_call = rospy.ServiceProxy('get_all_object_descriptions', GetAllObjectDescriptions)
    request = GetAllObjectDescriptionsRequest()
    response = get_all_object_descriptions_call(request)
    print 'GetAllObjectDescriptions service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetAllObjectDescriptions service call failed: %s" % e

def call_remove_geometry_model(id):
  try:
    rospy.wait_for_service('remove_geometry_model')
    call = rospy.ServiceProxy('remove_geometry_model', RemoveGeometryModel)
    request = RemoveGeometryModelRequest()
    request.id = id
    response = call(request)
    rospy.loginfo('RemoveGeometryModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("RemoveGeometryModel service call failed: %s" % e)
    return None

def call_get_object_descriptions_list():
  try:
    rospy.wait_for_service('get_object_descriptions_list')
    call = rospy.ServiceProxy('get_object_descriptions_list', GetObjectDescriptionsList)
    request = GetObjectDescriptionsListRequest()
    response = call(request)
    print 'GetObjectDescriptionsList service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectDescriptions service call failed: %s" % e

def call_update_geometry_model_pose(id, pose):
  try:
    rospy.wait_for_service('update_geometry_model_pose')
    call = rospy.ServiceProxy('update_geometry_model_pose', UpdateGeometryModelPose)
    request = UpdateGeometryModelPoseRequest()
    request.id = id
    request.pose = pose
    response = call(request)
    rospy.loginfo('UpdateGeometryModelPose service ended succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "UpdateGeometryModelPose service call failed: %s" % e

def call_update_and_transform_geometry_model_pose(id, pose):
  try:
    rospy.wait_for_service('update_and_transform_geometry_model_pose')
    call = rospy.ServiceProxy('update_and_transform_geometry_model_pose', UpdateGeometryModelPose)
    request = UpdateGeometryModelPoseRequest()
    request.id = id
    request.pose = pose
    response = call(request)
    rospy.loginfo('UpdateAndTransformGeometryModelPose service ended succeeded!')
    return response
  except rospy.ServiceException as e:
    return None, "UpdateAndTransformGeometryModelPose service call failed: %s" % e

def call_set_geometry_model_pose(id, pose):
  try:
    rospy.wait_for_service('set_geometry_model_pose')
    call = rospy.ServiceProxy('set_geometry_model_pose', SetGeometryModelPose)
    request = SetGeometryModelPoseRequest()
    request.id = id
    request.pose = pose
    response = call(request)
    print 'SetGeometryModelPose service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "SetGeometryModelPose service call failed: %s" % e

def call_get_geometry_model_bb(id):
  try:
    print 'wait da'
    rospy.wait_for_service('get_geometry_model_bb')
    call = rospy.ServiceProxy('get_geometry_model_bb', GetGeometryModelBoundingBox)
    request = GetGeometryModelBoundingBoxRequest()
    request.id = id
    print 'call da'
    response = call(request)
    print 'GetGeometryModelBoundingBox service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetGeometryModelBoundingBox service call failed: %s" % e

def call_get_object_description_types():
  try:
    rospy.wait_for_service('get_object_description_types')
    request = GetObjectDescriptionTypesRequest()
    call = rospy.ServiceProxy('get_object_description_types', GetObjectDescriptionTypes)
    response = call(request)
    print 'GetObjectDescriptionTypes service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectDescriptionTypes service call failed: %s" % e

def call_rename_geometry_model(id, type):
  try:
    rospy.wait_for_service('rename_geometry_model')
    request = RenameGeometryModelRequest()
    request.id = id
    request.type = type
    call = rospy.ServiceProxy('rename_geometry_model', RenameGeometryModel)
    response = call(request)
    print 'RenameGeometryModel service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "RenameGeometryModel service call failed: %s" % e

def call_get_geometry_model_types():
  try:
    rospy.wait_for_service('get_geometry_model_types')
    request = GetGeometryModelTypesRequest()
    call = rospy.ServiceProxy('get_geometry_model_types', GetGeometryModelTypes)
    response = call(request)
    print 'GetGeometryModelTypes service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetGeometryModelTypes service call failed: %s" % e

def call_get_frame_names():
  try:
    rospy.wait_for_service('get_frame_names')
    request = GetFrameNamesRequest()
    call = rospy.ServiceProxy('get_frame_names', GetFrameNames)
    response = call(request)
    print 'GetFrameNames service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetFrameNames service call failed: %s" % e

def call_update_object_descriptions(ids):
  try:
    rospy.wait_for_service('update_object_descriptions')
    request = UpdateObjectDescriptionsRequest()
    request.ids = ids
    call = rospy.ServiceProxy('update_object_descriptions', UpdateObjectDescriptions)
    response = call(request)
    print 'UpdateObjectDescriptions service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "UpdateObjectDescriptions service call failed: %s" % e

def call_make_relative3d(ids):
  try:
    rospy.wait_for_service('make_relative3d')
    request = UpdateObjectDescriptionsRequest()
    request.ids = ids
    call = rospy.ServiceProxy('make_relative3d', UpdateObjectDescriptions)
    response = call(request)
    print 'MakeRelative3D service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "MakeRelative3D service call failed: %s" % e

# add geometric primitives to a description

def call_add_point_2d_model(id, model):
  try:
    rospy.wait_for_service('add_point_2d_model')
    call = rospy.ServiceProxy('add_point_2d_model', AddPoint2DModel)
    request = AddPoint2DModelRequest()
    request.id = id
    request.model = model
    response = call(request)
    rospy.loginfo('AddPoint2DModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("AddPoint2DModel service call failed: %s" % e)
    return None

def call_add_pose_2d_model(id, model):
  try:
    rospy.wait_for_service('add_pose_2d_model')
    call = rospy.ServiceProxy('add_pose_2d_model', AddPose2DModel)
    request = AddPose2DModelRequest()
    request.id = id
    request.model = model
    response = call(request)
    rospy.loginfo('AddPose2DModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("AddPose2DModel service call failed: %s" % e)
    return None

def call_add_polygon_2d_model(id, model):
  try:
    rospy.wait_for_service('add_polygon_2d_model')
    call = rospy.ServiceProxy('add_polygon_2d_model', AddPolygon2DModel)
    request = AddPolygon2DModelRequest()
    request.id = id
    request.model = model
    response = call(request)
    rospy.loginfo('AddPolygon2DModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("AddPolygon2DModel service call failed: %s" % e)
    return None

def call_add_point_3d_model(id, model):
  try:
    rospy.loginfo('AddPoint3DModel service called!')
    rospy.wait_for_service('add_point_3d_model')
    call = rospy.ServiceProxy('add_point_3d_model', AddPoint3DModel)
    request = AddPoint3DModelRequest()
    request.id = id
    request.model = model
    response = call(request)
    rospy.loginfo('AddPoint3DModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("AddPoint3DModel service call failed: %s" % e)
    return None

def call_add_pose_3d_model(id, model):
  try:
    rospy.wait_for_service('add_pose_3d_model')
    call = rospy.ServiceProxy('add_pose_3d_model', AddPose3DModel)
    request = AddPose3DModelRequest()
    request.id = id
    request.model = model
    response = call(request)
    rospy.loginfo('AddPose3DModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("AddPose3DModel service call failed: %s" % e)
    return None

def call_add_polygon_3d_model(id, model):
  try:
    rospy.wait_for_service('add_polygon_3d_model')
    call = rospy.ServiceProxy('add_polygon_3d_model', AddPolygon3DModel)
    request = AddPolygon3DModelRequest()
    request.id = id
    request.model = model
    response = call(request)
    rospy.loginfo('AddPolygon3DModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("AddPolygon3DModel service call failed: %s" % e)
    return None

def call_add_triangle_mesh_3d_model(id, model):
  try:
    rospy.wait_for_service('add_triangle_mesh_3d_model')
    call = rospy.ServiceProxy('add_triangle_mesh_3d_model', AddTriangleMesh3DModel)
    request = AddTriangleMesh3DModelRequest()
    request.id = id
    request.model = model
    response = call(request)
    rospy.loginfo('AddTriangleMesh3DModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("AddTriangleMesh3DModel service call failed: %s" % e)
    return None
