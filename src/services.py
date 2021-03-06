#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'semap_ros' )

from semap_ros.srv import *

from tf_functions import *

from database_functions import *
from instance_functions import *
from description_functions import *

from spatial_relations import *
from topology_functions import *
from test_functions import *

from db_environment import db, initializeConnection

'''
SEMAP DB Services
'''

def semap_services():

  rospy.init_node( 'semap_db_services' )
  rospy.loginfo( "SEMAP DB Services are initializing...\n" )
  user = rospy.get_param( '~user' )
  password = rospy.get_param( '~password' )
  host = rospy.get_param( '~host' )
  database = rospy.get_param( '~database' )

  initializeConnection( user, password, host, database, False )

  ## Object Descriptions
  srv_add_object_descriptions = rospy.Service( 'add_object_descriptions', AddObjectDescriptions, add_object_descriptions )
  srv_copy_object_descriptions = rospy.Service( 'copy_object_descriptions', CopyObjectDescriptions, copy_object_descriptions )
  srv_delete_object_descriptions = rospy.Service( 'delete_object_descriptions', DeleteObjectDescriptions, delete_object_descriptions )
  srv_get_object_descriptions = rospy.Service( 'get_object_descriptions', GetObjectDescriptions, get_object_descriptions )
  srv_get_object_descriptions_ids = rospy.Service( 'get_object_descriptions_ids', GetObjectDescriptionsIDs, get_object_descriptions_ids )
  srv_get_object_descriptions_list = rospy.Service( 'get_object_descriptions_list', GetObjectDescriptionsList, get_object_descriptions_list )
  srv_get_all_object_descriptions = rospy.Service( 'get_all_object_descriptions', GetAllObjectDescriptions, get_all_object_descriptions )
  srv_rename_object_description = rospy.Service( 'rename_object_description', RenameObjectDescription, rename_object_description )
  srv_update_object_descriptions = rospy.Service( 'update_object_descriptions', UpdateObjectDescriptions, update_object_descriptions )
  srv_update_all_object_descriptions = rospy.Service( 'update_all_object_descriptions', UpdateObjectDescriptions, update_all_object_descriptions )

  ## Geometry Models
  srv_make_relative3d = rospy.Service( 'make_relative3d', UpdateObjectDescriptions, make_relative3d )
  srv_set_geometry_model = rospy.Service( 'set_geometry_model_pose', SetGeometryModelPose, set_geometry_model_pose )
  srv_update_geometry_model = rospy.Service( 'update_geometry_model_pose', UpdateGeometryModelPose, update_geometry_model_pose )
  srv_update_and_transform_geometry_model_pose = rospy.Service( 'update_and_transform_geometry_model_pose', UpdateGeometryModelPose, update_and_transform_geometry_model_pose )
  srv_rename_geometry_model = rospy.Service( 'rename_geometry_model', RenameGeometryModel, rename_geometry_model )
  srv_remove_geometry_model = rospy.Service( 'remove_geometry_model', RemoveGeometryModel, remove_geometry_model )
  srv_get_geometry_model_types = rospy.Service( 'get_geometry_model_types', GetGeometryModelTypes, get_geometry_model_types )
  srv_get_geometry_model_bb = rospy.Service( 'get_geometry_model_bb', GetGeometryModelBoundingBox, get_geometry_model_bb )
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
  srv_get_object_instances_ids = rospy.Service( 'get_object_instances_ids', GetObjectInstancesIDs, get_object_instances_ids )
  srv_get_object_instances_list = rospy.Service( 'get_object_instances_list', GetObjectInstancesList, get_object_instances_list )
  srv_get_all_object_instances = rospy.Service( 'get_all_object_instances', GetAllObjectInstances, get_all_object_instances )
  srv_rename_object_instance = rospy.Service( 'rename_object_instance', RenameObjectInstance, rename_object_instance )
  srv_delete_object_instances = rospy.Service( 'delete_object_instances', DeleteObjectInstances, delete_object_instances )
  srv_copy_object_instances = rospy.Service( 'copy_object_instances', CopyObjectInstances, copy_object_instances )
  srv_switch_object_descriptions = rospy.Service( 'switch_object_descriptions', SwitchObjectDescriptions, switch_object_descriptions )
  srv_update_absolute_descriptions = rospy.Service( 'update_absolute_descriptions', UpdateAbsoluteDescriptions, update_absolute_descriptions )

  ### TF Tree
  srv_add_root_frame = rospy.Service( 'add_root_frame', AddRootFrame, add_root_frame )
  srv_set_transform = rospy.Service( 'set_transform', SetTransform, set_transform )
  srv_update_transform = rospy.Service( 'update_transform', UpdateTransform, update_transform )
  srv_change_frame = rospy.Service( 'change_frame', ChangeFrame, change_frame )
  srv_get_frame_names = rospy.Service( 'get_frame_names', GetFrameNames, get_frame_names )
  srv_get_transform = rospy.Service( 'get_transform', GetTransform, get_transform )
  srv_get_children = rospy.Service( 'get_children', GetChildren, get_children )
  srv_get_env_children = rospy.Service( 'get_env_children', GetChildren, get_environment_children )

  ### NAV
  srv_get_absolute_footprint_polygons = rospy.Service( 'get_absolute_footprint_polygons', GetAbsoluteFootprintPolygons, get_absolute_footprint_polygons )
  srv_get_absolute_footprint_polygons = rospy.Service( 'get_absolute_body_meshes', GetAbsoluteBodyMeshes, get_absolute_body_meshes )

  ## Spatial Relations

  # manual
  srv_get_objects_within_range = rospy.Service( 'get_objects_within_range', GetObjectsWithinRange, get_objects_within_range )
  srv_get_objects_within_area = rospy.Service( 'get_objects_within_area', GetObjectsWithinArea, get_objects_within_area )
  srv_get_objects_within_volume = rospy.Service( 'get_objects_within_volume', GetObjectsWithinVolume, get_objects_within_volume )

  # one to many
  srv_get_objects_within_object = rospy.Service( 'get_objects_within_object', GetObjectsWithinObject, get_objects_within_object )
  #srv_get_objects_on_object = rospy.Service( 'get_objects_on_object', GetObjectsOnObject, get_objects_on_object )

  # many to many
  srv_get_objects_on_objects = rospy.Service( 'get_objects_on_objects', GetObjectsOnObjects, get_objects_on_objects )
  srv_get_objects_in_objects = rospy.Service( 'get_objects_in_objects', GetObjectsInObjects, get_objects_in_objects )
  srv_get_objects_intersect_objects = rospy.Service( 'get_objects_intersect_objects', GetObjectsIntersectObjects, get_objects_intersect_objects )

  srv_get_distance_between_objects = rospy.Service( 'get_distance_between_objects', GetDistanceBetweenObjects, get_distance_between_objects )

  ## Topology Management
  srv_structure_by_support = rospy.Service( 'structure_by_support', GetObjectsOnObjects, structure_by_support )
  srv_structure_by_containment = rospy.Service( 'structure_by_containment', GetObjectsInObjects, structure_by_containment )
  srv_unstructure_by_containment = rospy.Service( 'unstructure_by_containment', GetObjectsInObjects, unstructure_by_containment )

  # evaluation
  srv_evaluate_intersection = rospy.Service( 'evaluate_intersection', GetObjectsIntersectObjects, evaluate_intersection )
  srv_evaluate_containment = rospy.Service( 'evaluate_containment', GetObjectsInObjects, evaluate_containment )
  #srv_bind_objects_on_objects = rospy.Service( 'bind_objects_on_objects', GetObjectsWithinObject, bind_objects_on_objects )
  #srv_unbind_objects_on_objects = rospy.Service( 'unbind_objects_on_objects', GetObjectsWithinObject, unbind_objects_on_objects )
  #srv_get_objects_in_obb3d = rospy.Service( 'get_objects_in_obb3d', GetObjectsOnOBB3D, get_objects_in_obb3d )

  srv_get_directional_relations2d = rospy.Service( 'get_directional_relations2d', GetDirectionalRelations2D, get_directional_relations2d )
  srv_get_directional_relations3d = rospy.Service( 'get_directional_relations3d', GetDirectionalRelations2D, get_directional_relations3d )
  srv_test_containment_relations3d = rospy.Service( 'test_containment_relations3d', GetDirectionalRelations2D, test_containment_relations3d )
  #srv_get_objects_within_object_range3d = rospy.Service( 'get_objects_within_object_range3d', GetObjectsWithinRange, get_objects_within_object_range3d )

  ### UTIL

  srv_extrude_polygon = rospy.Service( 'extrude_polygon', ExtrudePolygon, extrude_polygon )

  ### DB
  srv_db_truncate_tables = rospy.Service( 'db_truncate_tables', DBTruncateTables, db_truncate_tables )
  srv_db_create_tables = rospy.Service( 'db_create_tables', DBCreateTables, db_create_tables )
  srv_db_drop_tables = rospy.Service( 'db_drop_tables', DBDropTables, db_drop_tables )
  srv_db_write_graph = rospy.Service( 'db_write_graph', DBDropTables, db_write_graph )

  ## Tests
  srv_test_create_absolute_description = rospy.Service( 'test_create_absolute_description', GetObjectInstances, test_create_absolute_description )
  srv_test_retrieval = rospy.Service( 'test_retrieval', GetObjectInstances, test_retrieval )
  srv_test_ecmr = rospy.Service( 'test_ecmr', GetObjectInstances, test_ecmr )
  srv_test_object_instances = rospy.Service( 'test_object_instances', GetObjectInstances, test_object_instances )
  srv_unary_relation_test = rospy.Service( 'unary_relation_test', UnaryRelationTest, unary_relation_test )
  srv_binary_relation_test = rospy.Service( 'binary_relation_test', BinaryRelationTest, binary_relation_test )


  rospy.loginfo( "SEMAP DB Services are running...\n" )
  rospy.spin()

if __name__ == "__main__":
  semap_services()
