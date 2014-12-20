#!/usr/bin/env python

'''
SpatialDB Service Calls
'''

import roslib; roslib.load_manifest('spatial_db')
import rospy
from db_model import *
from db_environment import db
from spatial_db_ros.srv import *

def add_root_frame(req):
    if create_root_node(req.frame):
      rospy.loginfo("SpatialDB SRVs: added root frame: %s", req.frame)
    else:
      rospy.logerr("SpatialDB SRVs: could not add root frame: %s", req.frame)
    return AddRootFrameResponse()

## description

def list_object_descriptions():
    descriptions = db().query(ObjectDescription.type, ObjectDescription.id).all()
    print "Available ObjectDescriptions"
    for type, id in descriptions:
     print id, type

def add_object_descriptions(req):
  res = AddObjectDescriptionsResponse()
  for desc in req.descriptions:
    object = ObjectDescription()
    object.fromROS(desc)
    db().add(object)
    db().flush()
    res.ids.append(object.id)
  db().commit()
  return res

def get_object_descriptions(req):
    res = GetObjectDescriptionsResponse()
    objects = db().query(ObjectDescription).filter(ObjectDescription.id.in_(req.ids)).all()
    for obj in objects:
      res.objects.append(obj.toROS())
    return res

def get_object_description_id(req):
    res = GetObjectDescriptionIDResponse()
    res.id = db().query(ObjectDescription.id).filter(ObjectDescription.type == req.type).scalar()
    return res

def get_all_object_descriptions(req):
    res = GetAllObjectDescriptionResponse()
    descriptions = db().query(ObjectDescription)
    for description in descriptions:
      res.descriptions.append(description.toROS())
    return res

## instances

def add_object_instances(req):
    res = AddObjectInstancesResponse()
    for obj in req.objects:
      object = ObjectInstance()
      object.fromROS(obj)
      db().add(object)
      db().flush()
      res.ids.append(object.id)
    db().commit()
    return res

def list_object_instances():
    objects = db().query(ObjectInstance.id, ObjectInstance.alias, ObjectDescription.type).filter(ObjectInstance.object_description_id == ObjectDescription.id).all()
    print "Available ObjectInstances"
    for id, alias, type in objects:
     print id, type, ':', alias

def rename_object_instance(id, alias):
    obj = db().query(ObjectInstance).filter(ObjectInstance.id == id).one()
    obj.alias = alias
    db().commit()

def get_object_instances(req):
    res = GetObjectInstancesResponse()
    objects = db().query(ObjectInstance).filter(ObjectInstance.id.in_(req.ids)).all()
    for obj in objects:
      res.objects.append(obj.toROS())
    return res

def get_all_object_instances(req):
    res = GetAllObjectInstancesResponse()
    objects = db().query(ObjectInstance)
    for obj in objects:
      print obj.object_description.type
      res.objects.append(obj.toROS())
    return res

def update_transform(req):
  res = UpdateTransformResponse()
  object = db().query(ObjectInstance).filter(ObjectInstance.id == req.id).one()
  print 'Update', object.object_description.type, object.id
  print 'before', fromStringToTransform(object.frame.transform)
  object.frame.appendROSPose(req.pose)
  db().commit()
  print 'after', fromStringToTransform(object.frame.transform)
  return res

def set_transform(req):
  res = SetTransformResponse()
  object = db().query(ObjectInstance).filter(ObjectInstance.id == req.id).scalar()
  print object.id
  print 'before', fromStringToTransform(object.frame.transform)
  object.frame.setROSPose(req.pose)
  db().commit()
  print 'after', fromStringToTransform(object.frame.transform)
  return res

def change_frame(req):
  res = ChangeFrame()
  object = db().query(ObjectInstance).filter(ObjectInstance.id == req.id).scalar()
  print object.id
  print 'before', object.frame.parent.name
  print 'before', fromStringToTransform(object.frame.transform)
  object.frame.changeFrame(req.frame, req.keep_transform)
  db().commit()
  object = db().query(ObjectInstance).filter(ObjectInstance.id == req.id).scalar()
  print object.id
  print 'after', object.frame.parent.name
  print 'after', fromStringToTransform(object.frame.transform)
  return res

### DB Maintainance

def db_truncate_tables(req):
    truncate_all()
    rospy.logwarn("SpatialDB SRVs: Truncated all tables.")
    return DBTruncateTablesResponse()

def db_create_tables(req):
    create_all()
    rospy.logwarn("SpatialDB SRVs: Created all tables.")
    return DBCreateTablesResponse()

def db_drop_tables(req):
    drop_all()
    rospy.logwarn("SpatialDB SRVs: Dropped all tables.")
    return DBDropTablesResponse()

