#!/usr/bin/env python

import roslib; roslib.load_manifest('spatial_db_ros')

import rospy

from spatial_db_ros.srv import *
from service_functions import *

def printDBContent(msg):
  print
  list_object_descriptions()
  print
  list_object_instances()

def spatial_db_services():

  rospy.init_node('spatial_db_services')

  rospy.Timer(rospy.Duration(5.0), printDBContent)

  ## Object Descriptions
  srv_add_object_descriptions = rospy.Service('add_object_descriptions', AddObjectDescriptions, add_object_descriptions)
  srv_get_object_descriptions = rospy.Service('get_object_descriptions', GetObjectDescriptions, get_object_descriptions)
  srv_get_object_description_id = rospy.Service('get_object_description_id', GetObjectDescriptionID, get_object_description_id)
  srv_get_all_object_descriptions = rospy.Service('get_all_object_descriptions', GetAllObjectDescriptions, get_all_object_descriptions)

  ## Object Instances
  srv_add_object_instances = rospy.Service('add_object_instances', AddObjectInstances, add_object_instances)
  srv_get_object_instances = rospy.Service('get_object_instances', GetObjectInstances, get_object_instances)
  srv_get_all_object_instances = rospy.Service('get_all_object_instances', GetAllObjectInstances, get_all_object_instances)

  ### TF TREE
  srv_add_root_frame = rospy.Service('add_root_frame', AddRootFrame, add_root_frame)
  srv_set_transform = rospy.Service('set_transform', SetTransform, set_transform)
  srv_update_transform = rospy.Service('update_transform', UpdateTransform, update_transform)
  srv_change_frame = rospy.Service('change_frame', ChangeFrame, change_frame)

  ### DB
  srv_db_truncate_tables = rospy.Service('db_truncate_tables', DBTruncateTables, db_truncate_tables)
  srv_db_create_tables = rospy.Service('db_create_tables', DBCreateTables, db_create_tables)
  srv_db_drop_tables = rospy.Service('db_drop_tables', DBDropTables, db_drop_tables)

  print "SpatialDB Services are online."

  rospy.spin()

if __name__ == "__main__":
  spatial_db_services()
