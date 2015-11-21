#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'semap_ros' )

from db_model import *
from db_environment import db
from semap_ros.srv import *

'''
SEMAP Transformations Services
'''

def add_root_frame( req ):
  if create_root_node( req.frame ):
    rospy.loginfo( "SEMAP DB SRVs: added root frame: %s", req.frame )
  else:
    rospy.logerr( "SEMAP DB SRVs: could not add root frame: %s", req.frame )
  return AddRootFrameResponse()

def get_transform( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_transform" )
  res = GetTransformResponse()
  res.transform = rosGetTransform( req.source, req.target )
  return res

def get_frame_names( req ):
  rospy.loginfo( "SEMAP DB SRVs: get frame names" )
  res = GetFrameNamesResponse()
  names = db().query( FrameNode.name ).all()
  for name in names:
    res.frames.append( str( name[0] ) )
  return res
