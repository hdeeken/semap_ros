#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'spatial_db_ros' )

from db_model import *
from spatial_db_ros.srv import *

'''
SEMAP DB Database Services
'''

def db_truncate_tables(req):
  truncate_all()
  rospy.loginfo( "SEMAP DB SRVs: Truncated all tables." )
  return DBTruncateTablesResponse()

def db_create_tables(req):
  create_all()
  rospy.loginfo( "SEMAP DB SRVs: Created all tables." )
  return DBCreateTablesResponse()

def db_drop_tables(req):
  drop_all()
  rospy.loginfo( "SEMAP DB SRVs: Dropped all tables." )
  return DBDropTablesResponse()
