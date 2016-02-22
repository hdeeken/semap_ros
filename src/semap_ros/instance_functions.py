#!/usr/bin/env python

import rospy
import roslib; roslib.load_manifest( 'semap_ros' )

from sqlalchemy.orm import aliased, join

from semap.db_model import *
from semap.db_environment import db

from semap_ros.srv import *
from semap_ros.subqueries import *
from semap_ros.instance_srv_calls import *
from semap.ros_postgis_conversion import *

from semap_msgs.msg import ObjectInstance as ROSObjectInstance

'''
SEMAP Object Instances Services
'''

## Setter

def add_object_instances( req ):
  rospy.loginfo( "SEMAP DB SRVs: add_object_instances" )
  res = AddObjectInstancesResponse()
  for obj in req.objects:
    object = ObjectInstance( obj )
    db().add( object )
    db().flush()
    res.ids.append( object.id )
  db().commit()
  call_update_absolute_descriptions(res.ids)
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
    obj.relative_description_id = req.desc_id
  db().commit()
  call_update_absolute_descriptions(req.obj_ids)
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

  if True:
    inst = aliased(ObjectInstance)
    desc = aliased(ObjectDescription)
    frame = aliased(FrameNode)

    descs = db().query( desc.id, desc.type, desc ).filter( inst.id.in_( req.ids ), inst.relative_description_id == desc.id ).distinct().all()
    descs_time = (rospy.Time.now() - then ).to_sec()
    #print descs
    #for id, type, desc in descs:
      #print id, type
    resultset ={}
    for id, type, desc in descs:
      resultset[id] = desc.toROS()
    #print resultset
    ros_time = (rospy.Time.now() - then ).to_sec() - descs_time
    insts = db().query( inst.id, inst.name, inst.alias, inst.relative_description_id, frame ).filter( inst.id.in_( req.ids ), inst.frame_id == frame.id ).all()
    insts_time = (rospy.Time.now() - then ).to_sec() - descs_time - ros_time

    for id, name, alias, desc_id, frame in insts:
      #print id, alias, desc_id, frame

      ros = ROSObjectInstance()
      ros.id = id
      ros.name = name

      if alias:
        ros.alias = alias
      else:
        ros.alias = ""

      if frame:
        ros.pose = frame.toROSPoseStamped()
      else:
        ros.pose = ROSPoseStamped()

      if desc_id:
        ros.description = resultset[desc_id]#.toROS()
      else:
        ros.description = ROSObjectDescription()

      if False: # parametrize if absolute desc should be returned
        rospy.loginfo( "# get absolute descriptions" )
        abs_desc = db().query( ObjectDescription ).filter( ObjectInstance.id == id, ObjectInstance.absolute_description_id == ObjectDescription.id ).scalar()
        ros.absolute = abs_desc.toROS()
      else:
        rospy.loginfo( "# skipping absolute descriptions" )
        ros.absolute = ROSObjectDescription()
      res.objects.append( ros )
    packaging_time = (rospy.Time.now() - then ).to_sec() - descs_time - ros_time- insts_time


    rospy.loginfo( "# Descriptions : %d" % len(descs) )
    rospy.loginfo( "# Instances    : %d" % len(insts) )

    rospy.loginfo( "  Descriptions  : %fs" % descs_time )
    rospy.loginfo( "  ROS Conversion: %fs" % ros_time )
    rospy.loginfo( "  Instances     : %fs" % insts_time)
    rospy.loginfo( "  Packaging     : %fs" % packaging_time )
    rospy.loginfo( "Total             %fs" % ( rospy.Time.now() - then ).to_sec() )

  then2 = rospy.Time.now()

  if False:
    rospy.loginfo( "SEMAP DB SRVs: get_object_instances NAIVE" )
    objects = db().query( ObjectInstance ).filter( ObjectInstance.id.in_( req.ids ) ).all()
    obj_time = (rospy.Time.now() - then2 ).to_sec()
    for obj in objects:
      ros = obj.toROS()
      res.objects.append( ros )
    packaging2_time = (rospy.Time.now() - then2 ).to_sec() - obj_time
    rospy.loginfo( "  Objects       : %fs" % obj_time )
    rospy.loginfo( "  Packaging     : %fs" % packaging2_time )
    rospy.loginfo( "Total             %fs" % ( rospy.Time.now() - then2 ).to_sec() )

  return res

def get_object_instances_ids( req ):
  rospy.loginfo( "DB SRVs: get_object_instances_ids" )
  res = GetObjectInstancesIDsResponse()
  res.ids = any_obj_types_ids(ObjectInstance, req.types)
  return res

def get_object_instances_list( req ):
  rospy.loginfo( "DB SRVs: get_object_instances_list" )
  res = GetObjectInstancesListResponse()
  objects = db().query( ObjectInstance.id, ObjectInstance.name, ObjectInstance.alias, FrameNode, ObjectDescription.type).\
                filter( ObjectInstance.id.in_( any_obj_types_ids(ObjectInstance, req.types) ), ObjectInstance.relative_description_id == ObjectDescription.id, ObjectInstance.frame_id == FrameNode.id ).all()
  for id, name, alias, frame, type in objects:
    obj = ROSObjectInstance()
    obj.id = id
    obj.name = name
    obj.alias = alias
    obj.pose = frame.toROSPoseStamped()
    obj.description.type = type
    res.objects.append( obj )
  return res

def list_object_instances():
  rospy.loginfo( "SEMAP DB SRVs: list_object_instances" )
  objects = db().query( ObjectInstance ).all()
  print "Available ObjectInstances"
  for obj in objects:
    print 'id:', obj.id, 'name:', obj.name, 'aka:', obj.alias, 'type:', obj.relative_description.type

def copy_object_instances( req ):
  rospy.loginfo( "SEMAP DB SRVs: copy_object_instances" )
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

# frame

def set_transform( req ):
  rospy.loginfo( "SEMAP DB SRVs: set transform" )
  res = SetTransformResponse()
  object = db().query( ObjectInstance ).filter(ObjectInstance.id == req.id).scalar()
  object.frame.setROSPose(req.pose)
  db().commit()
  update_res = call_update_absolute_descriptions( [ req.id ] )
  res.ids = update_res.ids
  return res

def update_transform( req ):
  rospy.loginfo( "SEMAP DB SRVs: update transform" )
  res = UpdateTransformResponse()
  object = db().query( ObjectInstance ).filter( ObjectInstance.id == req.id ).one()
  object.frame.appendROSPose( req.pose )
  db().commit()
  update_res = call_update_absolute_descriptions( [ req.id ] )
  res.ids = update_res.ids
  rospy.loginfo( "SEMAP DB SRVs: update transform finished" )
  return res

def change_frame( req ):
  rospy.logdebug( "SEMAP DB SRVs: change frame" )
  res = ChangeFrameResponse()
  object = db().query( ObjectInstance ).filter( ObjectInstance.id == req.id ).scalar()
  object.frame.changeFrame( req.frame, req.keep_transform )
  db().commit()
  if req.keep_transform:
    update_res = call_update_absolute_descriptions( [ req.id ] )
    res.ids = update_res.ids
  return res

def get_children( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_children" )
  res = GetChildrenResponse()
  then = rospy.Time.now()
  object = db().query( ObjectInstance ).filter(ObjectInstance.id == req.id).scalar()
  rospy.loginfo( "Get Children %fs" % ( rospy.Time.now() - then ).to_sec() )
  res.ids = object.getAnchestorIDs()
  return res

def get_environment_children( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_children" )
  res = GetChildrenResponse()
  then = rospy.Time.now()
  frame = db().query( FrameNode ).filter(FrameNode.name == "world").scalar()

  for key in frame.all_children.keys():
      child = frame.all_children[key]
      res.ids.append( child.object_instance.id )
  rospy.loginfo( "Get Env Children %fs" % ( rospy.Time.now() - then ).to_sec() )
  return res



# absolute

def update_absolute_descriptions( req ):
    rospy.loginfo( "SEMAP DB SRVs: update_absolute_description" )
    res = UpdateAbsoluteDescriptionsResponse()
    print 'update inst', req.ids
    objects = db().query( ObjectInstance ).filter( ObjectInstance.id.in_( req.ids ) ).all()
    for obj in objects:
      obj.updateAbsoluteDescription()
      res.ids.append( obj.id )
      if len( obj.getChildIDs() ):
        rospy.loginfo( "update children" )
        print obj.getChildIDs()
        child_res = call_update_absolute_descriptions( obj.getChildIDs() )
        res.ids += child_res.ids
    print 'updated these instances', res.ids
    return res

def get_absolute_body_meshes( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_absolute_body_meshes" )
  response = GetAbsoluteBodyMeshesResponse()

  obj = aliased( ObjectInstance )
  desc = aliased( ObjectDescription )
  geo = aliased( GeometryModel )

  for type in req.object_types:
    geos = db().query( geo ).filter(
       obj.id.in_( any_obj_type_ids( obj, type ) ),
       geo.id.in_( get_geometry( obj.id, geo, "Body" ) ) ).all()
    for model in geos:
      print model.geometry, model.type
      response.meshes.append( toTriangleMesh3D(model.geometry) )

  return response

def get_absolute_footprint_polygons( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_absolute_footprint_polygons" )
  response = GetAbsoluteFootprintPolygonsResponse()

  obj = aliased( ObjectInstance )
  desc = aliased( ObjectDescription )
  geo = aliased( GeometryModel )

  for type in req.object_types:
    geos = db().query( geo ).filter(
       obj.id.in_( any_obj_type_ids( obj, type ) ),
       geo.id.in_( get_abstraction( obj.id, geo, req.footprint_type ) ) ).all()

    for model in geos:
      if req.footprint_padding:
        response.polygons.append( toPolygon2D( db().execute( ST_Buffer( model.geometry , req.footprint_padding, 'endcap=square join=round') ).scalar() ) )
      else:
        response.polygons.append( toPolygon2D( model.geometry ) )

  return response

def get_instance_ids_by_type( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_instance_ids_by_type" )
  then = rospy.Time.now()
  res = GetObjectInstancesResponse()
  objects = any_obj_types_ids(obj, types)
  for obj in objects:
    then2 = rospy.Time.now()
    ros = obj.toROS()
    res.objects.append( ros )
    rospy.loginfo( "Object in %r seconds" % ( ( rospy.Time.now() - then2 ).to_sec() ) )
  rospy.loginfo( "Get Objects took %f seconds in total." % ( rospy.Time.now() - then ).to_sec() )
  return res
