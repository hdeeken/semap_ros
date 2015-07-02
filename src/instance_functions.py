#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'spatial_db_ros' )

from db_model import *
from db_environment import db
from spatial_db.ros_postgis_conversion import *
from spatial_db_ros.srv import *
from spatial_db_msgs.msg import ObjectDescription as ROSObjectDescription
from spatial_db_msgs.msg import ObjectInstance as ROSObjectInstance

'''
SEMAP DB Services for Object Instances
'''

## Setter

def add_object_instances( req ):
  rospy.loginfo( "SEMAP DB SRVs: add_object_instances"
  res = AddObjectInstancesResponse()
  for obj in req.objects:
    object = ObjectInstance( obj )
    db().add( object )
    db().flush()
    res.ids.append( object.id )
  db().commit()
  return res

def rename_object_instance( req ):
  rospy.loginfo( "SEMAP DB SRVs: rename_object_instance" )
  res = RenameObjectInstanceResponse()
  inst = db().query( ObjectInstance ).filter( ObjectInstance.id==req.id ).one()
  inst.alias = req.alias
  db().commit()
  return res

def switch_object_descriptions( req ):
  rospy.loginfo( "SEMAP DB SRVs: switch_object_descriptions" )
  res = SwitchObjectDescriptionsResponse()
  objects = db().query( ObjectInstance ).filter( ObjectInstance.id.in_( req.obj_ids ) ).all()
  for obj in objects:
    obj.object_description_id = req.desc_id
  db().commit()
  return res

def delete_object_instances( req ):
  rospy.loginfo( "SEMAP DB SRVs: delete_object_instances" )
  res = DeleteObjectInstancesResponse()
  objects = db().query( ObjectInstance ).filter( ObjectInstance.id.in_(req.ids) ).all()
  if len( objects ) > 0:
    for obj in objects:
      child_ids = obj.getChildIDs()
      if len( child_ids ) > 0:
        if not req.keep_children:
          child_req = DeleteObjectInstancesRequest()
          child_req.ids = child_ids
          child_req.keep_children = req.keep_children
          child_res = delete_object_instances( child_req )
          res.ids += child_res.ids
        else:
          for child in child_ids:
            frame_req = ChangeFrameRequest()
            frame_req.id = child
            if req.child_frame:
              frame_req.frame = req.child_frame
            else:
              frame_req.frame = obj.frame.parent.name
            frame_req.keep_transform = req.keep_transform
            change_frame(frame_req)
      obj.deleteAbsoluteDescription()
      db().delete( obj.frame )
      res.ids.append( obj.id )
      db().delete( obj )
    db().commit()
  return res

## Getter

def get_object_instances( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_object_instances" )
  then = rospy.Time.now()
  res = GetObjectInstancesResponse()
  objects = db().query( ObjectInstance ).filter( ObjectInstance.id.in_( req.ids ) ).all()
  for obj in objects:
    then2 = rospy.Time.now()
    ros = obj.toROS()
    print 'load', ros.absolute.type
    res.objects.append( ros )
    rospy.loginfo( "Object in %r seconds" % ( ( rospy.Time.now() - then2 ).to_sec() ) )
  rospy.loginfo( "Get Objects took %f seconds in total." % ( rospy.Time.now() - then ).to_sec() )
  return res

def get_object_instances_list( req ):
  rospy.loginfo( "DB SRVs: get_object_instances_list" )
  res = GetObjectInstancesListResponse()
  objects = db().query( ObjectInstance.id, ObjectInstance.name, ObjectInstance.alias, FrameNode ).filter( ObjectInstance.frame_id == FrameNode.id ).all()
  for id, name, alias, frame in objects:
    obj = ROSObjectInstance()
    obj.id = id
    obj.name = name
    obj.alias = alias
    obj.pose = frame.toROSPoseStamped()
    res.objects.append( obj )
  return res

def list_object_instances():
  objects = db().query( ObjectInstance ).all()
  print "Available ObjectInstances"
  for obj in objects:
    print 'id:', obj.id, 'name:', obj.name, 'aka:', obj.alias, 'type:', obj.object_description.type

def copy_object_instances( req ):
  # get all objects to be copied
  get_req = GetObjectInstancesRequest()
  get_req.ids = req.ids
  get_res = get_object_instances( get_req )
  # and just add them again
  for obj in get_res.objects:
    obj.alias = "Copy of " + obj.name
  add_req = AddObjectInstancesRequest()
  add_req.objects = get_res.objects
  add_res = add_object_instances( add_req )
  #return the ids
  res = CopyObjectInstancesResponse()
  res.ids = add_res.ids
  return res

def get_all_object_instances( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_all_object_instances" )
  then = rospy.Time.now()
  res = GetAllObjectInstancesResponse()
  objects = db().query( ObjectInstance ).all()
  rospy.loginfo( "Get All Objects from DB took %f seconds" % ( rospy.Time.now() - then ).to_sec() )
  for obj in objects:
    res.objects.append( obj.toROS() )
  rospy.loginfo( "Get All Objects as ROS took %f seconds" % ( rospy.Time.now() - then ).to_sec() )
  return res

# instance

def update_transform( req ):
  rospy.loginfo( "SEMAP DB SRVs: update transform" )
  res = UpdateTransformResponse()
  object = db().query( ObjectInstance ).filter( ObjectInstance.id == req.id ).one()
  object.frame.appendROSPose( req.pose )
  db().commit()
  return res

def set_transform( req ):
  rospy.loginfo( "SEMAP DB SRVs: set transform" )
  res = SetTransformResponse()
  object = db().query( ObjectInstance ).filter(ObjectInstance.id == req.id).scalar()
  object.frame.setROSPose(req.pose)
  db().commit()
  return res

def change_frame( req ):
  rospy.loginfo( "SEMAP DB SRVs: change frame" )
  res = ChangeFrameResponse()
  object = db().query( ObjectInstance ).filter( ObjectInstance.id == req.id ).scalar()
  object.frame.changeFrame( req.frame, req.keep_transform )
  db().commit()
  return res

def test_create_absolute_description( req ):
    rospy.loginfo( "SEMAP DB SRVs: test_create_absolute_description" )
    then = rospy.Time.now()
    res = GetObjectInstancesResponse()
    objects = db().query( ObjectInstance ).filter( ObjectInstance.id.in_( req.ids ) ).all()
    for obj in objects:
      print "Create Absolute for", obj.name
      obj.createAbsoluteDescription()
    return res
