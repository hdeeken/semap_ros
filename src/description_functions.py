#!/usr/bin/env python

import roslib; roslib.load_manifest( 'semap_ros' )
import rospy
from sqlalchemy import exc
from sqlalchemy.orm import aliased, join

from db_model import *
from db_environment import db

from semap.ros_postgis_conversion import *
from semap_msgs.msg import ObjectDescription as ROSObjectDescription

from semap_ros.srv import *
from semap_ros.instance_srv_calls import *
from semap_ros.description_srv_calls import *
from instance_functions import *

'''
SEMAP Object Descriptions Services
'''

def add_object_descriptions( req ):
  rospy.loginfo( "SEMAP DB SRVs: add object descriptions" )
  res = AddObjectDescriptionsResponse()
  for desc in req.descriptions:
    object = ObjectDescription()
    object.fromROS(desc)
    db().add( object )
    db().flush()
    res.ids.append( object.id )
  db().commit()
  call_update_object_descriptions(res.ids)
  rospy.loginfo( "SEMAP DB SRVs: add object descriptions - DONE" )
  return res

def delete_object_descriptions( req ):
  rospy.loginfo( "SEMAP DB SRVs: delete_object_descriptions" )
  res = DeleteObjectDescriptionsResponse()
  descs = db().query( ObjectDescription).filter( ObjectDescription.id.in_( req.ids ) ).all()
  if len( descs ) > 0:
    for desc in descs:
      inst_ids = desc.getInstancesIDs()
      if len( inst_ids ) > 0:
        res.ids += inst_ids
        if not req.keep_instances:
          child_req = DeleteObjectInstancesRequest()
          child_req.ids = inst_ids
          child_req.keep_children = True
          child_res = delete_object_instances( child_req )
          res.ids += child_res.ids
        else:
          if req.new_desc_id != 0:
            request = SwitchObjectDescriptionsRequest()
            request.obj_ids = inst_ids
            request.desc_id = req.new_desc_id
            switch_object_descriptions( request )
          else:
              objects = db().query( ObjectInstance ).filter( ObjectInstance.id.in_( inst_ids ) ).all()
              for obj in objects:
                obj.description_id = None
      db().delete( desc )
    db().commit()
  return res

def copy_object_descriptions( req ):
  rospy.loginfo( "SEMAP DB SRVs: copy_object_descriptions" )
  # get all objects to be copied
  get_req = GetObjectDescriptionsRequest()
  get_req.ids = req.ids
  get_res = get_object_descriptions( get_req )
  # and just add them again
  for desc in get_res.descriptions:
    desc.type = "Copy of " + desc.type
  add_req = AddObjectDescriptionsRequest()
  add_req.descriptions = get_res.descriptions
  add_res = add_object_descriptions( add_req )
  #return the ids
  res = CopyObjectDescriptionsResponse()
  res.ids = add_res.ids
  return res

def rename_object_description( req ):
  rospy.loginfo( "SEMAP DB SRVs: rename_object_description" )
  res = RenameObjectDescriptionResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  desc.type = req.type
  db().commit()
  update_res = call_update_object_descriptions( [ req.id ] )
  res.ids = update_res.ids
  return res

# getter

def get_object_descriptions( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_object_descriptions" )
  then = rospy.Time.now()
  res = GetObjectDescriptionsResponse()
  descriptions = db().query( ObjectDescription ).filter( ObjectDescription.id.in_( req.ids ) ).all()
  for desc in descriptions:
    res.descriptions.append( desc.toROS() )
  rospy.loginfo( "Took %f seconds" % ( rospy.Time.now() - then).to_sec() )
  return res

def get_object_descriptions_ids( req ):
  rospy.loginfo( "DB SRVs: get_object_descriptions_ids" )
  res = GetObjectDescriptionsIDsResponse()
  print ' desc types', req.types
  ids = desc_ids_by_type(ObjectDescription, req.types).all()
  res.ids = [id for id, in ids]
  return res

def get_object_descriptions_list( req ):
  rospy.loginfo( "SEMAP DB SRVs: get_object_descriptions_list" )
  res = GetObjectDescriptionsListResponse()
  descriptions =[]
  try:
    ids = desc_ids_by_type(ObjectDescription, req.types).all()
    descriptions = db().query(ObjectDescription.id, ObjectDescription.type).distinct().filter(ObjectDescription.id.in_( ids ), ~ObjectDescription.type.match('absolute_description_')).all()
    for id, type in descriptions:
      desc = ROSObjectDescription()
      desc.id = id
      desc.type = type
      res.descriptions.append(desc)
    rospy.loginfo( "return proper res" )
    return res
  except exc.SQLAlchemyError, e:
      if len(descriptions) == 0:
        rospy.loginfo( "no desc found" )
      rospy.loginfo( "sql error %s" % e)
      rospy.loginfo( "got %d descriptions" % len( descriptions ) )
  rospy.loginfo( "return empty res" )
  return res

def list_object_descriptions():
  descriptions = db().query( ObjectDescription.type, ObjectDescription.id ).all()
  print "Available ObjectDescriptions"
  for type, id in descriptions:
   print id, type

def get_all_object_descriptions( req ):
  rospy.loginfo( "SEMAP DB SRVs: get all object descriptions" )
  res = GetAllObjectDescriptionResponse()
  descriptions = db().query( ObjectDescription ).filter( ObjectDescription.id != ObjectInstance.absolute_description_id).all()
  for description in descriptions:
    res.descriptions.append( description.toROS() )
  return res

def get_geometry_model_types( req ):
  rospy.logdebug( "SEMAP DB SRVs: get_geometry_model_types" )
  res = GetGeometryModelTypesResponse()
  abstractions_types = ['Position2D', 'Position3D', 'AxisAligned2D', 'AxisAligned3D', 'FootprintBox', 'FootprintHull', 'BoundingBox', 'BoundingHull', 'Extrusion']

  types = db().query( GeometryModel.type ).distinct().filter( or_(*[GeometryModel.type.contains(type) for type in abstractions_types]) ).all()
  #types = db().query( GeometryModel.type ).distinct().filter( ~GeometryModel.type.in_(abstractions_types) ).all()
  for t in types:
    res.types.append( t[0] )
  return res

def rename_geometry_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: rename_geometry_model" )
  res = RenameGeometryModelResponse()
  model = db().query( GeometryModel ).filter( GeometryModel.id == req.id ).one()
  model.type = req.type
  db().commit()
  update_res = call_update_object_descriptions( [ model.geometry_desc ] )
  res.ids = update_res.ids
  return res

def remove_geometry_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: remove_geometry_model" )
  res = RemoveGeometryModelResponse()
  model = db().query( GeometryModel ).filter( GeometryModel.id == req.id ).one()
  db().delete( model )
  db().commit()
  update_res = call_update_object_descriptions( [ model.geometry_desc ] )
  res.ids = update_res.ids
  return res

def update_geometry_model_pose( req ):
  rospy.logdebug( "SEMAP DB SRVs: update_geometry_model_pose" )
  res = UpdateGeometryModelPoseResponse()
  model = db().query( GeometryModel ).filter( GeometryModel.id == req.id ).one()
  model.pose.appendROSPose( req.pose )
  db().commit()
  update_res = call_update_object_descriptions( [ model.geometry_desc ] )
  res.ids = update_res.ids
  return res

def update_and_transform_geometry_model_pose( req ):
  rospy.logdebug( "SEMAP DB SRVs: update_and_transform_geometry_model_pose" )
  res = UpdateGeometryModelPoseResponse()
  model = db().query( GeometryModel ).filter( GeometryModel.id == req.id ).one()
  model.applyROSPose(req.pose)
  db().commit()
  update_res = call_update_object_descriptions( [ model.geometry_desc ] )
  res.ids = update_res.ids
  return res

def set_geometry_model_pose( req ):
  rospy.logdebug( "SEMAP DB SRVs: set_geometry_model_pose" )
  res = SetGeometryModelPoseResponse()
  model = db().query( GeometryModel ).filter( GeometryModel.id == req.id ).one()
  model.pose.fromROS( req.pose )
  db().commit()
  update_res = call_update_object_descriptions( [ model.geometry_desc ] )
  res.ids = update_res.ids
  return res

def get_geometry_model_bb( req ):
  rospy.logdebug( "SEMAP DB SRVs: get_geometry_model_bb" )
  res = GetGeometryModelBoundingBoxResponse()
  model = db().query( GeometryModel ).filter( GeometryModel.id == req.id ).one()
  vals = model.getBoundingBoxValues()
  res.min_x = vals[0]
  res.min_y = vals[1]
  res.min_z = vals[2]
  res.max_x = vals[3]
  res.max_y = vals[4]
  res.max_z = vals[5]
  return res

def update_all_object_descriptions( req ):
  #delete_all_absolute_descriptions( req )
  rospy.loginfo( "SEMAP DB SRVs: update_all_object_descriptions" )
  req = UpdateObjectDescriptionsRequest()
  res = UpdateObjectDescriptionsResponse()
  relative_ids = db().query( ObjectDescription.id ).filter( ~ObjectDescription.type.match('absolute_description_') ).all()
  req.ids = [id for id, in relative_ids]
  print req.ids
  res = update_object_descriptions(req)
  return res

def delete_all_absolute_descriptions( req ):
  rospy.logdebug( "SEMAP DB SRVs: delete_all" )
  req = UpdateObjectDescriptionsRequest()
  res = UpdateObjectDescriptionsResponse()
  descs = db().query( ObjectDescription ).filter( ObjectDescription.type.match('absolute_description_') ).all()
  for desc in descs:
    print 'Delete', desc.type
    db().delete( desc )
  db().commit()
  return res

def update_object_descriptions( req ):
  rospy.loginfo( "SEMAP DB SRVs: update_object_descriptions" )
  res = UpdateObjectDescriptionsResponse()
  descriptions = db().query( ObjectDescription ).filter( ObjectDescription.id.in_( req.ids ) ).all()
  for desc in descriptions:
    print 'Update', desc.type
    desc.updateAbstractions()
  instances = db().query( ObjectInstance).filter( ObjectInstance.relative_description_id.in_( req.ids ) ).all()
  instance_ids = []
  for inst in instances:
    print 'Update', inst.name
    instance_ids.append(inst.id)
  instance_res = call_update_absolute_descriptions(instance_ids)
  res.ids = instance_res.ids
  return res

def make_relative3d( req ):
  rospy.logdebug( "SEMAP DB SRVs: make_relative3d" )
  res = UpdateObjectDescriptionsResponse()
  bb_res = call_get_geometry_model_bb(req.ids[0])
  pose = ROSPose()
  pose.position.x =  - bb_res.max_x  + ( bb_res.max_x - bb_res.min_x ) / 2
  pose.position.y =  - bb_res.max_y  + ( bb_res.max_y - bb_res.min_y ) / 2
  pose.position.z =  0.0  -  bb_res.min_z
  tmp = call_update_and_transform_geometry_model_pose(req.ids[0], pose)
  for i in tmp.ids:
    pose.position.x *= -1
    pose.position.y *= -1
    pose.position.z *= -1
    call_update_transform(i, pose)
  return res

# add geometric primitives to a description

def add_point_2d_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: add_point_2d_model" )
  res = AddPoint2DModelResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  res.id = desc.addPoint2DModel( req.model )
  db().commit()
  call_update_object_descriptions( [ req.id ] )
  return res

def add_pose_2d_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: add_pose_2d_model" )
  res = AddPose2DModelResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  res.id = desc.addPose2DModel( req.model )
  db().commit()
  call_update_object_descriptions( [ req.id ] )
  return res

def add_polygon_2d_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: add_polygon_2d_model" )
  res = AddPolygon2DModelResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  res.id = desc.addPolygon2DModel( req.model )
  db().commit()
  call_update_object_descriptions( [ req.id ] )
  return res

def add_point_3d_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: add_point_3d_model" )
  res = AddPoint3DModelResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  res.id = desc.addPoint3DModel( req.model )
  db().commit()
  call_update_object_descriptions( [ req.id ] )
  return res

def add_pose_3d_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: add_pose_3d_model" )
  res = AddPose3DModelResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  res.id = desc.addPose3DModel( req.model )
  db().commit()
  call_update_object_descriptions( [ req.id ] )
  return res

def add_polygon_3d_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: add_polygon_3d_model" )
  res = AddPolygon3DModelResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  res.id = desc.addPolygon3DModel( req.model )
  db().commit()
  call_update_object_descriptions( [ req.id ] )
  return res

def add_triangle_mesh_3d_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: add_triangle_mesh_3d_model" )
  then = rospy.Time.now()
  res = AddTriangleMesh3DModelResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  res.id = desc.addTriangleMesh3DModel( req.model )
  db().commit()
  call_update_object_descriptions( [ req.id ] )
  return res

def add_polygon_mesh_3d_model( req ):
  rospy.logdebug( "SEMAP DB SRVs: add_polygon_mesh_3d_model" )
  res = AddPolygonMesh3DModelResponse()
  desc = db().query( ObjectDescription ).filter( ObjectDescription.id == req.id ).one()
  res.id = desc.addPolygonMesh3DModel( req.model )
  db().commit()
  call_update_object_descriptions( [ req.id ] )
  return res
