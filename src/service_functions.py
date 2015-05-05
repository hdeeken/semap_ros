#!/usr/bin/env python

'''
SpatialDB Service Calls
'''

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
  rospy.loginfo("SpatialDB SRVs: add object descriptions")
  res = AddObjectDescriptionsResponse()
  for desc in req.descriptions:
    object = ObjectDescription()
    object.fromROS(desc)
    db().add(object)
    db().flush()
    res.ids.append(object.id)
  db().commit()
  rospy.loginfo("SpatialDB SRVs: add object descriptions - DONE")
  return res

def get_object_descriptions(req):
    rospy.loginfo("SpatialDB SRVs: get_object_descriptions")
    then = rospy.Time.now()
    res = GetObjectDescriptionsResponse()
    descriptions = db().query(ObjectDescription).filter(ObjectDescription.id.in_(req.ids)).all()
    for desc in descriptions:
      res.descriptions.append(desc.toROS())
    rospy.loginfo("Took %f seconds" % (rospy.Time.now() - then).to_sec())
    return res

def copy_object_descriptions(req):
    # get all objects to be copied
    get_req = GetObjectDescriptionsRequest()
    get_req.ids = req.ids
    get_res = get_object_descriptions(get_req)
    # and just add them again

    for desc in get_res.descriptions:
      desc.type = "Copy of " + desc.type

    add_req = AddObjectDescriptionsRequest()
    add_req.descriptions = get_res.descriptions
    add_res = add_object_descriptions(add_req)
    #return the ids
    res = CopyObjectDescriptionsResponse()
    res.ids = add_res.ids
    return res

def get_object_description_id(req):
    rospy.loginfo("SpatialDB SRVs: get object description id")
    res = GetObjectDescriptionIDResponse()
    res.id = db().query(ObjectDescription.id).filter(ObjectDescription.type == req.type).scalar()
    return res

def get_all_object_descriptions(req):
    rospy.loginfo("SpatialDB SRVs: get all object descriptions")
    res = GetAllObjectDescriptionResponse()
    descriptions = db().query(ObjectDescription).all()
    for description in descriptions:
      res.descriptions.append(description.toROS())
    return res

def get_geometry_model_types(req):
    rospy.loginfo("SpatialDB SRVs: get_geometry_model_types")
    res = GetGeometryModelTypesResponse()
    types = db().query(GeometryModel.type).all()
    for t in types:
      res.types.append(t[0])
    return res

def delete_object_descriptions(req):
    rospy.loginfo("SpatialDB SRVs: delete_object_descriptions")
    res = DeleteObjectDescriptionsResponse()
    descs = db().query(ObjectDescription).filter(ObjectDescription.id.in_(req.ids)).all()

    if len(descs) > 0:
      for desc in descs:
        inst_ids = desc.getInstancesIDs()
        print inst_ids
        if len(inst_ids) > 0:
          res.ids += inst_ids
          if not req.keep_instances:
            child_req = DeleteObjectInstancesRequest()
            child_req.ids = inst_ids
            child_req.keep_children = True
            child_res = delete_object_instances(child_req)
            res.ids += child_res.ids
          else:
            if req.new_desc_id != 0:
              request = SwitchObjectDescriptionsRequest()
              request.obj_ids = inst_ids
              request.desc_id = req.new_desc_id
              switch_object_descriptions(request)
            else:
                objects = db().query(ObjectInstance).filter(ObjectInstance.id.in_(inst_ids)).all()
                for obj in objects:
                  obj.description_id = None

        for model in desc.geometry_models:
          db().delete(model.pose)
          db().delete(model)
        db().delete(desc)
      db().commit()
    return res

def delete_object_instances(req):
    rospy.loginfo("SpatialDB SRVs: delete_object_instances")
    res = DeleteObjectInstancesResponse()
    objects = db().query(ObjectInstance).filter(ObjectInstance.id.in_(req.ids)).all()
    if len(objects) > 0:
      for obj in objects:
        child_ids = obj.getChildIDs()
        if len(child_ids) > 0:
          if not req.keep_children:
            child_req = DeleteObjectInstancesRequest()
            child_req.ids = child_ids
            child_req.keep_children = req.keep_children
            child_res = delete_object_instances(child_req)
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
        db().delete(obj.frame)
        res.ids.append(obj.id)
        db().delete(obj)
      db().commit()
    return res

def rename_geometry_model(req):
    rospy.loginfo("SpatialDB SRVs: rename_geometry_model")
    res = RenameGeometryModelResponse()
    model = db().query(GeometryModel).filter(GeometryModel.id==req.id).one()
    model.type = req.type
    db().commit()
    return res

def rename_object_description(req):
    rospy.loginfo("SpatialDB SRVs: rename_object_description")
    res = RenameObjectDescriptionResponse()
    desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
    desc.type = req.type
    db().commit()
    return res

def rename_object_instance(req):
    rospy.loginfo("SpatialDB SRVs: rename_object_instance")
    res = RenameObjectInstanceResponse()
    inst = db().query(ObjectInstance).filter(ObjectInstance.id==req.id).one()
    inst.alias = req.alias
    print 'rename', req.id, 'to', req.alias
    db().commit()
    return res

def remove_geometry_model(req):
    rospy.loginfo("SpatialDB SRVs: remove_geometry_model")
    res = RemoveGeometryModelResponse()
    print 'desc id', req.id, 'model type', req.type
    model = db().query(GeometryModel).filter(GeometryModel.object_description_id==req.id, GeometryModel.type==req.type).one()
    db().delete(mo)
    db().commit()
    return res

def remove_geometry_model(req):
    rospy.loginfo("SpatialDB SRVs: remove_geometry_model")
    res = RemoveGeometryModelResponse()
    print 'desc id', req.id, 'model type', req.type
    model = db().query(GeometryModel).filter(GeometryModel.object_description_id==req.id, GeometryModel.type==req.type).all()
    for mo in model:
      db().delete(mo)
    db().commit()
    return res

def add_point_2d_model(req):
  rospy.loginfo("SpatialDB SRVs: add_point_2d_model")
  res = AddPoint2DModelResponse()
  desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
  desc.addPoint2DModel(req.model)
  db().commit()
  return res

def add_pose_2d_model(req):
  rospy.loginfo("SpatialDB SRVs: add_pose_2d_model")
  res = AddPose2DModelResponse()
  desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
  desc.addPose2DModel(req.model)
  db().commit()
  return res

def add_polygon_2d_model(req):
  rospy.loginfo("SpatialDB SRVs: add_polygon_2d_model")
  res = AddPolygon2DModelResponse()
  desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
  desc.addPolygon2DModel(req.model)
  db().commit()
  return res

def add_point_3d_model(req):
  rospy.loginfo("SpatialDB SRVs: add_point_3d_model")
  res = AddPoint3DModelResponse()
  desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
  desc.addPoint3DModel(req.model)
  db().commit()
  return res

def add_pose_3d_model(req):
  rospy.loginfo("SpatialDB SRVs: add_pose_3d_model")
  res = AddPose3DModelResponse()
  desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
  desc.addPose3DModel(req.model)
  db().commit()
  return res

def add_polygon_3d_model(req):
  rospy.loginfo("SpatialDB SRVs: add_polygon_3d_model")
  res = AddPolygon3DModelResponse()
  desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
  desc.addPolygon3DModel(req.model)
  db().commit()
  return res

def add_triangle_mesh_3d_model(req):
  rospy.loginfo("SpatialDB SRVs: add_triangle_mesh_3d_model")
  then = rospy.Time.now()
  res = AddTriangleMesh3DModelResponse()
  desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
  desc.addTriangleMesh3DModel(req.model)
  db().commit()
  rospy.loginfo("Took %f seconds" % (rospy.Time.now() - then).to_sec())
  return res

def add_polygon_mesh_3d_model(req):
  rospy.loginfo("SpatialDB SRVs: add_polygon_mesh_3d_model")
  res = AddPolygonMesh3DModelResponse()
  desc = db().query(ObjectDescription).filter(ObjectDescription.id==req.id).one()
  desc.addPolygonMesh3DModel(req.model)
  db().commit()
  return res

## instances

def add_object_instances(req):
    rospy.loginfo("Add Object Instances")
    res = AddObjectInstancesResponse()
    for obj in req.objects:
      object = ObjectInstance(obj)
      db().add(object)
      db().flush()
      res.ids.append(object.id)
    db().commit()
    return res

def list_object_instances():
    objects = db().query(ObjectInstance).all()
    print "Available ObjectInstances"
    for obj in objects:
      print 'id:', obj.id, 'name:', obj.name, 'aka:', obj.alias, 'type:', obj.object_description.type

def get_object_instances(req):
    rospy.loginfo("SpatialDB SRVs: get_object_instances")
    then = rospy.Time.now()
    res = GetObjectInstancesResponse()
    objects = db().query(ObjectInstance).filter(ObjectInstance.id.in_(req.ids)).all()
    for obj in objects:
          then2 = rospy.Time.now()
          ros = obj.toROS()
          res.objects.append(ros)
          rospy.loginfo("Object in %r seconds" % ((rospy.Time.now() - then2).to_sec()))
    rospy.loginfo("Get Objects took %f seconds in total." % (rospy.Time.now() - then).to_sec())
    return res

def switch_object_descriptions(req):
    res = SwitchObjectDescriptionsResponse()
    objects = db().query(ObjectInstance).filter(ObjectInstance.id.in_(req.obj_ids)).all()
    for obj in objects:
      obj.object_description_id = req.desc_id
    db().commit()
    return res

def get_object_instances_list(req):
    res = GetObjectInstancesListResponse()
    objects = db().query(ObjectInstance.id, ObjectInstance.name, ObjectInstance.alias, FrameNode).filter(ObjectInstance.frame_id == FrameNode.id).all()
    for id, name, alias, frame in objects:
      obj = ROSObjectInstance()
      obj.id = id
      obj.name = name
      obj.alias = alias
      obj.pose = frame.toROSPoseStamped()
      res.objects.append(obj)
    return res

def get_object_descriptions_list(req):
    rospy.loginfo("SpatialDB SRVs: get_object_descriptions_list")
    res = GetObjectDescriptionsListResponse()
    rospy.loginfo("before SpatialDB SRVs: get_object_descriptions_list")
    descriptions =[]
    #import logging
    try:
      descriptions = db().query(ObjectDescription.id, ObjectDescription.type).all()

      for id, type in descriptions:
        desc = ROSObjectDescription()
        desc.id = id
        desc.type = type
        res.descriptions.append(desc)

      rospy.loginfo("return proper res")
      return res

    except exc.SQLAlchemyError, e:
        if len(descriptions) == 0:
          rospy.loginfo("no desc found")
        rospy.loginfo("sql error %s" % e)
        rospy.loginfo('got %d descriptions' % len(descriptions))

    rospy.loginfo("return empty res")
    return res

def copy_object_instances(req):
    # get all objects to be copied
    get_req = GetObjectInstancesRequest()
    get_req.ids = req.ids
    get_res = get_object_instances(get_req)
    # and just add them again

    for obj in get_res.objects:
      obj.alias = "Copy of " + obj.name

    add_req = AddObjectInstancesRequest()
    add_req.objects = get_res.objects
    add_res = add_object_instances(add_req)
    #return the ids
    res = CopyObjectInstancesResponse()
    res.ids = add_res.ids
    return res

def get_all_object_instances(req):
  rospy.loginfo("SpatialDB SRVs: get_all_object_instances")
  then = rospy.Time.now()
  res = GetAllObjectInstancesResponse()
  objects = db().query(ObjectInstance).all()
  rospy.loginfo("Get All Objects from DB took %f seconds" % (rospy.Time.now() - then).to_sec())
  for obj in objects:
    res.objects.append(obj.toROS())
  rospy.loginfo("Get All Objects as ROS took %f seconds" % (rospy.Time.now() - then).to_sec())
  return res

### Spatial Relations

def get_absolute_geometry(id, type):
  #rospy.loginfo("SpatialDB SRVs: get_absolute_geometry")
  then = rospy.Time.now()
  obj = db().query(ObjectInstance).filter(ObjectInstance.id == id).one()
  print obj.id, obj.name, obj.object_description.type

  for model in obj.object_description.geometry_models:
    #print model.type
    if model.type == type:
      break

  #then2 = rospy.Time.now()
  #geometry = model.geometry
  #rospy.loginfo("Look: %fs" % (rospy.Time.now() - then2).to_sec())

  then3 = rospy.Time.now()
  transformed_model = model.transformed()
  #rospy.loginfo("Trans: %fs" % (rospy.Time.now() - then3).to_sec())

  then4 = rospy.Time.now()
  absolute_model = obj.frame.apply(transformed_model)
  #rospy.loginfo("Abs: %fs" % (rospy.Time.now() - then4).to_sec())

  #print 'raw         ', db().execute(ST_AsText(geometry)).scalar()
  #print 'transformed ', db().execute(ST_AsText(transformed_model)).scalar()
  #print 'absolute    ', db().execute(ST_AsText(absolute_model)).scalar()
  #rospy.loginfo("Total: %fs" % (rospy.Time.now() - then).to_sec())

  return absolute_model

def unary_relation_test(req):

  geo = get_absolute_geometry(req.id, req.type)

  if "area" or "all" in req.relations:
    area = db().execute(SFCGAL_Area(ST_AsText(geo))).scalar()
    print 'area:', area

  if "area3d" or "all" in req.relations:
    area = db().execute(SFCGAL_3DArea(ST_AsText(geo))).scalar()
    print 'area:3d', area

  if "volume" or "all" in req.relations:
    area = db().execute(SFCGAL_Volume(ST_AsText(geo))).scalar()
    print 'volume:', area

  if "convexhull" or "all" in req.relations:
      hull = db().execute(ST_AsText(SFCGAL_3DConvexhull(geo))).scalar()
      print 'convexhull:', hull

def binary_relation_test(req):
  print
  print 'Called BinaryRelationsTest with', req.relations

  res = BinaryRelationTestResponse()
  geo1 = get_absolute_geometry(req.id1, req.type1)
  geo2 = get_absolute_geometry(req.id2, req.type2)


  #print 'GEO1'
  #print db().execute(ST_AsText(geo1)).scalar()
  #print 'GEO2'
  #print db().execute(ST_AsText(geo1)).scalar()

  result_collections = []


  #if "intersects" or "all" in req.relations:
    #intersects = db().execute(SFCGAL_3DIntersects(ST_AsText(geo1),ST_AsText(geo2))).scalar()
    #print 'intersects:', intersects

  if "intersection" in req.relations or "all" in req.relations:
    print
    print "INTERSECTION"
    data = db().execute(ST_AsText(SFCGAL_3DIntersection(geo1,geo2))).scalar()
    result = LabeledGeometryCollection()
    addToCollection(data, result.collection)
    result.label = 'Intersection'
    res.results.append(result)


  if "difference" in req.relations or "all" in req.relations:
    print
    print "DIFFERENCE"
    data = db().execute(ST_AsText(SFCGAL_3DDifference(ST_AsText(geo1),ST_AsText(geo2)))).scalar()
    result2 = LabeledGeometryCollection()
    print data
    addToCollection(data, result2.collection)
    result2.label = 'Difference'
    res.results.append(result2)


  if "union" in req.relations or "all" in req.relations:
    print
    print "UNION"
    data = db().execute(ST_AsText(SFCGAL_3DUnion(ST_AsText(geo1),ST_AsText(geo2)))).scalar()
    print data
    result3 = LabeledGeometryCollection()
    addToCollection(data, result3.collection)
    result3.label = 'Union'
    res.results.append(result3)

  return res

### Frames

def update_transform(req):
  rospy.loginfo("SpatialDB SRVs: update transform")
  res = UpdateTransformResponse()
  object = db().query(ObjectInstance).filter(ObjectInstance.id == req.id).one()
  object.frame.appendROSPose(req.pose)
  db().commit()
  return res

def set_transform(req):
  rospy.loginfo("SpatialDB SRVs: set transform")
  res = SetTransformResponse()
  object = db().query(ObjectInstance).filter(ObjectInstance.id == req.id).scalar()
  object.frame.setROSPose(req.pose)
  db().commit()
  return res

def update_geometry_model_pose(req):
  rospy.loginfo("SpatialDB SRVs: update_geometry_model_pose")
  res = UpdateGeometryModelPose()
  model = db().query(GeometryModel).filter(GeometryModel.id == req.id).one()
  model.pose.appendROSPose(req.pose)
  db().commit()
  return res

def update_and_transform_geometry_model_pose(req):
  rospy.loginfo("SpatialDB SRVs: update_geometry_model_pose")
  res = UpdateGeometryModelPose()
  model = db().query(GeometryModel).filter(GeometryModel.id == req.id).one()
  model.pose.appendROSPose(req.pose)
  model.geometry.appendROSPose(req.pose)
  db().commit()
  return res

def set_geometry_model_pose(req):
  rospy.loginfo("SpatialDB SRVs: set_geometry_model_pose")
  res = SetTransformResponse()
  model = db().query(GeometryModel).filter(GeometryModel.id == req.id).scalar()
  model.pose.fromROS(req.pose)
  db().commit()
  return res

def change_frame(req):
  rospy.loginfo("SpatialDB SRVs: change frame")
  res = ChangeFrameResponse()
  object = db().query(ObjectInstance).filter(ObjectInstance.id == req.id).scalar()
  object.frame.changeFrame(req.frame, req.keep_transform)
  db().commit()
  return res

def get_transform(req):
  rospy.loginfo("SpatialDB SRVs: get_transform")
  res = GetTransformResponse()
  res.transform = rosGetTransform(req.source, req.target)
  return res

def get_frame_names(req):
  rospy.loginfo("SpatialDB SRVs: get frame names")
  res = GetFrameNamesResponse()
  names = db().query(FrameNode.name).all()
  for name in names:
    res.frames.append(str(name[0]))
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

