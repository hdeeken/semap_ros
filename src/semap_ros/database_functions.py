#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'semap_ros' )

from semap.db_model import *

from semap_ros.srv import *

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

def db_write_graph(req):
  write_graph()
  rospy.loginfo( "SEMAP DB SRVs: Wrote DB graph." )
  return DBDropTablesResponse()
