#!/usr/bin/env python

import roslib; roslib.load_manifest('spatial_db_ros')
import rospy
from sqlalchemy import exc

from db_model import *
from db_environment import db
from spatial_db.ros_postgis_conversion import *
from spatial_db_ros.srv import *
from spatial_db_msgs.msg import ObjectDescription as ROSObjectDescription
from spatial_db_msgs.msg import ObjectInstance as ROSObjectInstance
from visualization_msgs.msg import MarkerArray

'''
SEMAP DB Service Calls for Transformations
'''

def add_root_frame(req):
  if create_root_node(req.frame):
    rospy.loginfo("SEMAP DB SRVs: added root frame: %s", req.frame)
  else:
    rospy.logerr("SEMAP DB SRVs: could not add root frame: %s", req.frame)
  return AddRootFrameResponse()

def get_transform(req):
  rospy.loginfo("SEMAP DB SRVs: get_transform")
  res = GetTransformResponse()
  res.transform = rosGetTransform(req.source, req.target)
  return res

def get_frame_names(req):
  rospy.loginfo("SEMAP DB SRVs: get frame names")
  res = GetFrameNamesResponse()
  names = db().query(FrameNode.name).all()
  for name in names:
    res.frames.append(str(name[0]))
  return res
