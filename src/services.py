#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'spatial_db_ros' )

from spatial_db_ros.srv import *

from tf_functions import *
from test_functions import *
from database_functions import *
from instance_functions import *
from description_functions import *

from db_environment import db, initializeConnection

'''
SEMAP DB Services
'''

def spatial_db_services():

  rospy.init_node( 'semap_db_services' )

  user = rospy.get_param( '~user' )
  password = rospy.get_param( '~password' )
  host = rospy.get_param( '~host' )
  database = rospy.get_param( '~database' )

  initializeConnection( user, password, host, database, False )

  ## Object Descriptions
  srv_add_object_descriptions = rospy.Service( 'add_object_descriptions', AddObjectDescriptions, add_object_descriptions )
  srv_get_object_descriptions = rospy.Service( 'get_object_descriptions', GetObjectDescriptions, get_object_descriptions )
  srv_get_object_description_id = rospy.Service( 'get_object_description_id', GetObjectDescriptionID, get_object_description_id )
  srv_get_all_object_descriptions = rospy.Service( 'get_all_object_descriptions', GetAllObjectDescriptions, get_all_object_descriptions )
  srv_get_object_descriptions_list = rospy.Service( 'get_object_descriptions_list', GetObjectDescriptionsList, get_object_descriptions_list )
  srv_delete_object_descriptions = rospy.Service( 'delete_object_descriptions', DeleteObjectDescriptions, delete_object_descriptions )
  srv_copy_object_descriptions = rospy.Service( 'copy_object_descriptions', CopyObjectDescriptions, copy_object_descriptions )
  srv_rename_object_description = rospy.Service( 'rename_object_description', RenameObjectDescription, rename_object_description )

  ## Geometry Models
  srv_set_geometry_model = rospy.Service( 'set_geometry_model_pose', SetGeometryModelPose, set_geometry_model_pose )
  srv_update_geometry_model = rospy.Service( 'update_geometry_model_pose', UpdateGeometryModelPose, update_geometry_model_pose )
  srv_rename_geometry_model = rospy.Service( 'rename_geometry_model', RenameGeometryModel, rename_geometry_model )
  srv_remove_geometry_model = rospy.Service( 'remove_geometry_model', RemoveGeometryModel, remove_geometry_model )
  srv_get_geometry_model_types = rospy.Service( 'get_geometry_model_types', GetGeometryModelTypes, get_geometry_model_types )
  srv_add_point_2d_model = rospy.Service( 'add_point_2d_model', AddPoint2DModel, add_point_2d_model )
  srv_add_pose_2d_model = rospy.Service( 'add_pose_2d_model', AddPose2DModel, add_pose_2d_model )
  srv_add_polygon_2d_model = rospy.Service( 'add_polygon_2d_model', AddPolygon2DModel, add_polygon_2d_model )
  srv_add_point_3d_model = rospy.Service( 'add_point_3d_model', AddPoint3DModel, add_point_3d_model )
  srv_add_pose_3d_model = rospy.Service( 'add_pose_3d_model', AddPose3DModel, add_pose_3d_model )
  srv_add_polygon_3d_model = rospy.Service( 'add_polygon_3d_model', AddPolygon3DModel, add_polygon_3d_model )
  srv_add_triangle_mesh_3d_model = rospy.Service( 'add_triangle_mesh_3d_model', AddTriangleMesh3DModel, add_triangle_mesh_3d_model )
  srv_add_polygon_mesh_3d_model = rospy.Service( 'add_polygon_mesh_3d_model', AddPolygonMesh3DModel, add_polygon_mesh_3d_model )

  ## Object Instances
  srv_add_object_instances = rospy.Service( 'add_object_instances', AddObjectInstances, add_object_instances )
  srv_get_object_instances = rospy.Service( 'get_object_instances', GetObjectInstances, get_object_instances )
  srv_get_object_instances_list = rospy.Service( 'get_object_instances_list', GetObjectInstancesList, get_object_instances_list )
  srv_get_all_object_instances = rospy.Service( 'get_all_object_instances', GetAllObjectInstances, get_all_object_instances )
  srv_rename_object_instance = rospy.Service( 'rename_object_instance', RenameObjectInstance, rename_object_instance )
  srv_delete_object_instances = rospy.Service( 'delete_object_instances', DeleteObjectInstances, delete_object_instances )
  srv_copy_object_instances = rospy.Service( 'copy_object_instances', CopyObjectInstances, copy_object_instances )
  srv_switch_object_descriptions = rospy.Service( 'switch_object_descriptions', SwitchObjectDescriptions, switch_object_descriptions )

  ### TF Tree
  srv_add_root_frame = rospy.Service( 'add_root_frame', AddRootFrame, add_root_frame )
  srv_set_transform = rospy.Service( 'set_transform', SetTransform, set_transform )
  srv_update_transform = rospy.Service( 'update_transform', UpdateTransform, update_transform )
  srv_change_frame = rospy.Service( 'change_frame', ChangeFrame, change_frame )
  srv_get_frame_names = rospy.Service( 'get_frame_names', GetFrameNames, get_frame_names )
  srv_get_transform = rospy.Service( 'get_transform', GetTransform, get_transform )

  ### DB
  srv_db_truncate_tables = rospy.Service( 'db_truncate_tables', DBTruncateTables, db_truncate_tables )
  srv_db_create_tables = rospy.Service( 'db_create_tables', DBCreateTables, db_create_tables )
  srv_db_drop_tables = rospy.Service( 'db_drop_tables', DBDropTables, db_drop_tables )

  ## Tests
  srv_test_create_absolute_description = rospy.Service( 'test_create_absolute_description', GetObjectInstances, test_create_absolute_description )
  srv_test_retrieval = rospy.Service( 'test_retrieval', GetObjectInstances, test_retrieval )
  srv_test_object_instances = rospy.Service( 'test_object_instances', GetObjectInstances, test_object_instances )
  srv_unary_relation_test = rospy.Service( 'unary_relation_test', UnaryRelationTest, unary_relation_test )
  srv_binary_relation_test = rospy.Service( 'binary_relation_test', BinaryRelationTest, binary_relation_test )

  print "SEMAP DB Services are online."
  rospy.spin()

if __name__ == "__main__":
  spatial_db_services()
