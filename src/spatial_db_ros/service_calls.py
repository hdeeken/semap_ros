#!/usr/bin/env python

'''
SpatialDB Service Calls
'''

import roslib; roslib.load_manifest('spatial_db_ros')
import rospy

from spatial_db_ros.srv import *

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

def call_rename_object_instance(id, alias):
  try:
    rospy.wait_for_service('rename_object_instance')
    request = RenameObjectInstanceRequest()
    request.id = id
    request.alias = alias
    call = rospy.ServiceProxy('rename_object_instance', RenameObjectInstance)
    response = call(request)
    print 'RenameObjectInstance service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "RenameObjectInstance service call failed: %s" % e

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

def call_delete_object_instances(ids, keep_children = True, child_frame = "", keep_transform = False):
  print 'call_delete_object_instances'
  try:
    rospy.wait_for_service('delete_object_instances')
    request = DeleteObjectInstancesRequest()
    request.ids = ids
    request.keep_children = keep_children
    request.child_frame = child_frame
    request.keep_transform = keep_transform
    call = rospy.ServiceProxy('delete_object_instances', DeleteObjectInstances)
    response = call(request)
    print 'DeleteObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "DeleteObjectInstances service call failed: %s" % e

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

def call_remove_geometry_model(id, type):
  try:
    rospy.wait_for_service('remove_geometry_model')
    call = rospy.ServiceProxy('remove_geometry_model', RemoveGeometryModel)
    request = RemoveGeometryModelRequest()
    request.id = id
    request.type = type
    response = call(request)
    rospy.loginfo('RemoveGeometryModel service call succeeded!')
    return response
  except rospy.ServiceException as e:
    rospy.loginfo("RemoveGeometryModel service call failed: %s" % e)
    return None

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

#inst
def call_add_object_instances(objects):
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

def call_copy_object_instances(ids):
  try:
    rospy.wait_for_service('copy_object_instances')
    request = CopyObjectInstancesRequest()
    request.ids = ids
    call = rospy.ServiceProxy('copy_object_instances', CopyObjectInstances)
    response = call(request)
    print 'CopyObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "CopyObjectInstances service call failed: %s" % e

def call_copy_object_descriptions(ids):
  try:
    rospy.wait_for_service('copy_object_descriptions')
    request = CopyObjectDescriptionsRequest()
    request.ids = ids
    call = rospy.ServiceProxy('copy_object_descriptions', CopyObjectDescriptions)
    response = call(request)
    print 'CopyObjectDescriptions service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "CopyObjectDescriptions service call failed: %s" % e

def call_get_object_instances(ids):
  try:
    rospy.wait_for_service('get_all_object_instances')
    call = rospy.ServiceProxy('get_object_instances', GetObjectInstances)
    request = GetObjectInstancesRequest()
    request.ids = ids
    response = call(request)
    print 'GetObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectInstances service call failed: %s" % e

def call_get_all_object_instances():
  try:
    rospy.wait_for_service('get_all_object_instances')
    get_all_object_instances_call = rospy.ServiceProxy('get_all_object_instances', GetAllObjectInstances)
    request = GetAllObjectInstancesRequest()
    response = get_all_object_instances_call(request)
    print 'GetAllObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetAllObjectInstances service call failed: %s" % e

def call_switch_object_descriptions(obj_ids, desc_id):
  try:
    rospy.wait_for_service('switch_object_descriptions')
    request = SwitchObjectDescriptionsRequest()
    request.obj_ids = obj_ids
    request.desc_id = desc_id
    call = rospy.ServiceProxy('switch_object_descriptions', SwitchObjectDescriptions)
    response = call(request)
    print 'SwitchObjectDescriptions service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "SwitchObjectDescriptions service call failed: %s" % e

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

def call_get_object_instances_list():
  try:
    rospy.wait_for_service('get_object_instances_list')
    call = rospy.ServiceProxy('get_object_instances_list', GetObjectInstancesList)
    request = GetObjectInstancesListRequest()
    response = call(request)
    print 'GetObjectInstancesList service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetObjectInstances service call failed: %s" % e

def call_activate_all_object_instances():
  try:
    rospy.wait_for_service('activate_all_object_instances')
    activate_all_object_instances_call = rospy.ServiceProxy('activate_all_object_instances', ActivateAllObjectInstances)
    request = ActivateAllObjectInstancesRequest()
    response = activate_all_object_instances_call(request)
    print 'ActivateAllObjectInstances service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "ActivateAllObjectInstances service call failed: %s" % e

def call_update_transform(id, pose):
  try:
    rospy.wait_for_service('update_transform')
    update_transform_call = rospy.ServiceProxy('update_transform', UpdateTransform)
    request = UpdateTransformRequest()
    request.id = id
    request.pose = pose
    response = update_transform_call(request)
    print 'UpdateTransform service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "UpdateTransform service call failed: %s" % e

def call_update_geometry_model_pose(id, pose):
  try:
    rospy.wait_for_service('update_geometry_model_pose')
    call = rospy.ServiceProxy('update_geometry_model_pose', UpdateGeometryModelPose)
    request = UpdateGeometryModelPoseRequest()
    request.id = id
    request.pose = pose
    response = call(request)
    print 'UpdateGeometryModelPose service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "UpdateGeometryModelPose service call failed: %s" % e

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

def call_set_transform(id, pose):
  try:
    rospy.wait_for_service('set_transform')
    call = rospy.ServiceProxy('set_transform', SetTransform)
    request = SetTransformRequest()
    request.id = id
    request.pose = pose
    response = call(request)
    print 'SetTransform service call succeeded!'
    return True
  except rospy.ServiceException as e:
    return None, "SetTransform service call failed: %s" % e

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

def call_change_frame(id, frame, keep_transform):
  try:
    rospy.wait_for_service('change_frame')
    call = rospy.ServiceProxy('change_frame', ChangeFrame)
    request = ChangeFrameRequest()
    request.id = id
    request.frame = frame
    request.keep_transform = keep_transform
    response = call(request)
    print 'ChangeFrame service call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "ChangeFrame service call failed: %s" % e

def call_get_transform(source, target):
  try:
    rospy.wait_for_service('get_transform')
    call = rospy.ServiceProxy('get_transform', GetTransform)
    request = GetTransformRequest()
    request.source = source
    request.target = target
    response = call(request)
    print 'GetTransform services call succeeded!'
    return response
  except rospy.ServiceException as e:
    return None, "GetTransform service call failed: %s" % e

###
### Spatial Relations
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
