#!/usr/bin/env python

import roslib; roslib.load_manifest('spatial_db_ros')

from sqlalchemy import Column, Integer, String
from sqlalchemy.sql import func
from sqlalchemy.orm import aliased

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement, WKBElement, RasterElement, CompositeElement
from geoalchemy2.functions import ST_Distance, ST_AsText
from postgis_functions import *

from db_environment import db
from db_model import *

from geometry_msgs.msg import Point as ROSPoint
from geometry_msgs.msg import Point32 as ROSPoint32
from geometry_msgs.msg import Pose2D as ROSPose2D
from geometry_msgs.msg import Pose as ROSPose
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
from visualization_msgs.msg import Marker, MarkerArray
from tf.transformations import quaternion_matrix, random_quaternion, quaternion_from_matrix, euler_from_matrix
from tf.broadcaster import TransformBroadcaster
from rviz_spatial_db_plugin import *
import rospy

active_objects = []
tf_broadcaster = None
publisher = None

def create_mesh_model_from_file(req):
    print req.path
    model = GeometryModel()
    model.type = "Imported Mesh"
    model.geometry_type = "MESH3D"
    model.geometry = WKTElement(importFromFile(req.path))
    object = ObjectDescription()
    object.type = req.type
    object.geometry_models.append(model)
    db().add(object)
    db().commit()
    return

def create_root_frame(req):
    if create_root_node(req.name):
      rospy.loginfo("SpatialDB SRVs: created root frame: %s", req.name)
    else:
      rospy.logerr("SpatialDB SRVs: could not create root frame: %s", req.name)
    return

def add_object_descriptions(req):
    for desc in req.descriptions:
      print desc.type, 'will be added'
      object = ObjectDescription()
      object.fromROS(desc)
      db().add(object)
    db().commit()
    return

def add_object_instances(req):
    print 'add', len(req.objects), 'objects into db'
    for obj in req.objects:
      object = ObjectInstance()
      object.fromROS(obj)
      print 'now i add'
      db().add(object)
    db().commit()
    print 'everything was alright'
    res = AddObjectInstancesResponse()
    return res

def get_all_object_instances(req):
    res = GetAllObjectInstancesResponse()
    db_objects = db().query(ObjectInstance)
    for db_object in db_objects:
      print db_object
      ros_object = db_object.toROS()
      res.objects.append(ros_object)
      active_objects.append(ros_object)
      print 'alrighty'
    print 'everything was alrighty'
    print res
    return res

def truncate_all_tables(req):
    truncate_all()
    return

def create_database(req):
    create_all()
    return

def drop_database(req):
    drop_all()
    return

def publishTF(msg):
    global active_objects
    if len(active_objects) > 0:

      print len(active_objects)
      for object in active_objects:
        position = object.pose.pose.position
        orientation = object.pose.pose.orientation
        time = rospy.Time.now()
        tf_broadcaster.sendTransform( (position.x, position.y, position.x), \
                                      (orientation.x, orientation.x, orientation.z, orientation.w), \
                                      time, object.description.type + str(object.id), "world")
        #print 'send transform for ', object.description.type + str(object.id)
      #print 'updated all transforms'

def publishMarker(msg):
    global active_objects
    if len(active_objects) > 0:
      for object in active_objects:
        array = create_object_description_marker(object.description, object.description.type + str(object.id))
        publisher.publish(array)
        #print 'publish'
      #print 'published all markers'

def spatial_db_services():
    global tf_broadcaster, publisher
    rospy.init_node('spatial_db_services')
    tf_broadcaster = TransformBroadcaster()
    rospy.Timer(rospy.Duration(0.05), publishTF)
    rospy.Timer(rospy.Duration(0.50), publishMarker)

    publisher = rospy.Publisher('object_marker', MarkerArray, queue_size=50)

    #object descriptions
    # total num
    # list of types
    # list of model_types per type

    ## Object Description
    #srv_object_description_overview
    #srv_all_object_description
    #srv_get_all_object_description
    #srv_remove_object_description
    srv_add_object_descriptions = rospy.Service('add_object_descriptions', AddObjectDescriptions, add_object_descriptions)

    ## Object Instances
    srv_add_object_instances = rospy.Service('add_object_instances', AddObjectInstances, add_object_instances)
    srv_get_all_object_instances = rospy.Service('get_all_object_instances', GetAllObjectInstances, get_all_object_instances)

    ### TF TREE
    srv_create_database = rospy.Service('create_root_frame', CreateRootFrame, create_root_frame)

    ### DB 
    srv_truncate_all_tables = rospy.Service('truncate_all_tables', TruncateAllTables, truncate_all_tables)
    srv_create_database = rospy.Service('create_database', CreateDatabase, create_database)
    srv_drop_database = rospy.Service('drop_database', DropDatabase, drop_database)

    print "SpatialDB Services are online."
    rospy.spin()

if __name__ == "__main__":
    #create_all()
    #drop_all()
    truncate_all()
    spatial_db_services()
