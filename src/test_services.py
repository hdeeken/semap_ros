#!/usr/bin/env python

import roslib; roslib.load_manifest('spatial_db_ros')

from geometry_msgs.msg import Point as ROSPoint
from geometry_msgs.msg import Point32 as ROSPoint32
from geometry_msgs.msg import Pose2D as ROSPose2D
from geometry_msgs.msg import Pose as ROSPose
from geometry_msgs.msg import PoseStamped as ROSPoseStamped
from geometry_msgs.msg import PoseStamped as ROSPoseStamped
from geometry_msgs.msg import Polygon as ROSPolygon
from shape_msgs.msg import Mesh as ROSMesh
from shape_msgs.msg import MeshTriangle as ROSMeshTriangle
from spatial_db_msgs.msg import PolygonMesh as ROSPolygonMesh
from spatial_db_msgs.msg import Point2DModel, Point3DModel, Pose2DModel, Pose3DModel, Polygon2DModel, Polygon3DModel, TriangleMesh3DModel, PolygonMesh3DModel
from spatial_db_msgs.msg import ObjectDescription as ROSObjectDescription
from spatial_db_msgs.msg import ObjectInstance as ROSObjectInstance
from spatial_db_msgs.msg import ObjectInstanceOverview as ROSObjectInstanceOverview

from spatial_db_ros.srv import *
from tf.transformations import quaternion_matrix, random_quaternion, quaternion_from_matrix, euler_from_matrix, euler_matrix, quaternion_from_euler

import rospy

from visualization_msgs.msg import Marker, MarkerArray
#from rviz_spatial_db_plugin import *
from assimp_postgis_importer import importFromFileToMesh
from object_description_utils import *
from service_calls import *

def test_add_dummy_descriptions():
  descs = []
  descs.append(create_complete_object_description())
  descs.append(create_object_description("Wuson", [[0,0,0], [0,0,0,1]], '/home/hdeeken/ros/indigo/hdeeken-dry/spatial_db_ros/data/wuson.ply'))
  descs.append(create_object_description("Tree" , [[0,0,0], [0,0,0,1]], '/home/hdeeken/ros/indigo/hdeeken-dry/spatial_db_ros/data/tree.ply'))

  add_object_descriptions(descs)

def test_add_dummy_instances():

  objs = []
  objs.append(create_object_instance("world", [[5,5,0], [0,0,0,1]], "wuson", get_object_description_id("Wuson").id))
  objs.append(create_object_instance("world", [[5,-5,0], [0,0,0,1]], "wussi", get_object_description_id("Wuson").id))
  objs.append(create_object_instance("world", [[-5,5,0], [0,0,0,1]], "wussel", get_object_description_id("Wuson").id))
  objs.append(create_object_instance("world", [[-5,-5,0], [0,0,0,1]], "wusso", get_object_description_id("Wuson").id))

  objs.append(create_object_instance("world", [[0,0,0], [0,0,0,1]], "geo", get_object_description_id("Geometry").id))
  add_object_instances(objs)

def create_object_instance(frame, pose, alias, description_id):
  pose_ = ROSPoseStamped()
  pose_.header.frame_id = frame
  pose_.pose.position.x = pose[0][0]
  pose_.pose.position.y = pose[0][1]
  pose_.pose.position.z = pose[0][2]
  pose_.pose.orientation.x = pose[1][0]
  pose_.pose.orientation.y = pose[1][1]
  pose_.pose.orientation.z = pose[1][2]
  pose_.pose.orientation.w = pose[1][3]

  inst_ros = ROSObjectInstance()
  inst_ros.alias = alias
  inst_ros.pose = pose_
  inst_ros.description.id = description_id
  return inst_ros

## Test Cases

if __name__ == "__main__":
  print 'Testing SpatialDB Services -- BEGINS'

  test_add_dummy_descriptions()
  add_root_frame("world2")

  #test_add_dummy_instances()

 # set_transform(15, [[-4,0,0],[0,0,0,1]])
 # set_transform(16, [[4,0,1],[0,0,0,1]])
  #update_transform()
  #change_frame(16, "geo2", False)
  #change_frame(16, "world", True)

  #get_all_object_instances()
  add_object_instances([create_object_instance("world", [[0,0,6], [0,0,0,1]], "geo2", get_object_description_id("Geometry").id)])
  #add_object_instances([create_object_instance("world", [[-10,0,6], [0,0,0,1]], "787", get_object_description_id("Geometry").id)])
  #activate_all_object_instances()

  print 'Testing SpatialDB Services -- DONE'
