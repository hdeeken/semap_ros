#!/usr/bin/env python

'''
SEMAP DB Service Calls
'''

import roslib; roslib.load_manifest('semap_ros')
import rospy
from sqlalchemy import exc

from db_model import *
from db_environment import db
from semap.ros_postgis_conversion import *
from semap_ros.srv import *
from semap_msgs.msg import ObjectDescription as ROSObjectDescription
from semap_msgs.msg import ObjectInstance as ROSObjectInstance
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
        obj.deleteAbsoluteDescription()
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
          print 'load', ros.absolute.type
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

### Tests
from sqlalchemy.orm import aliased, join
#>>> adalias1 = aliased(Address)
#>>> adalias2 = aliased(Address)
#sql
#>>> for username, email1, email2 in \
#...     session.query(User.name, adalias1.email_address, adalias2.email_address).\
#...     join(adalias1, User.addresses).\
#...     join(adalias2, User.addresses).\
#...     filter(adalias1.email_address=='jack@google.com').\
#...     filter(adalias2.email_address=='j25@yahoo.com'):
#...     print username, email1, email2

def test_retrieval(req):
    rospy.loginfo("SpatialDB SRVs: test_distance")
    then = rospy.Time.now()

    origin = WKTElement('POINT(%f %f %f)' % (0.0, 0.0, 0.0))

    geo0 = aliased(GeometryModel)
    geo1 = aliased(GeometryModel)
    obj1 = aliased(ObjectInstance)
    obj2 = aliased(ObjectInstance)

    anyobj =  db().query( geo0.geometry ).filter( geo0.type == "Body", obj1.absolute_description_id == geo0.object_description_id ).label( "any" )
    res0 =  db().query( geo0.geometry ).filter( obj1.id == 2, geo0.type == "Body", obj1.absolute_description_id == geo0.object_description_id ).label( "res0" )
    res1 =  db().query( geo1.geometry ).filter( obj2.id == 3, geo1.type == "Body", obj2.absolute_description_id == geo1.object_description_id ).label( "res1" )

    root_dist = SFCGAL_Distance3D( origin, res0 ).label("root_dist")
    in_root_range = db().query( obj1.id, root_dist ).filter(root_dist > 2.0)
    print in_root_range.all()
    
    #obj_dist = SFCGAL_Distance3D( res0, res1 ).label("obj_dist")
    #in_obj_range = db().query( obj1.id, obj2.id, obj_dist ).filter(obj_dist > 0.0)
    #print in_obj_range.all()

    intersects = db().query( obj1.id, obj2.id, SFCGAL_Intersects3D( res0, res1 ) ).filter(obj1.id != obj2.id)
    print intersects.all()

    #dist = db().query( obj1.id,  ).filter(SFCGAL_Distance3D( WKTElement('POINT(%f %f %f)' % (0.0, 0.0, 0.0)), res0 ) > 2.0)#.label("dist")
    #inrange = db().query( obj1 ).filter( SFCGAL_Distance3D( WKTElement('POINT(%f %f %f)' % (0.0, 0.0, 0.0)), res0 ) > 2.0 )#.label("dist")
    
    #inrange = db().query( obj1.id).filter( dist > 0.0).all()
    #print dist
    #for o in dist:
    #  print o
    
    #resres0 =  db().query(geo0.geometry).select_from(join(geo0, ObjectInstance)).filter( ObjectInstance.id == 3, ObjectInstance.absolute_description_id == GeometryModel.object_description_id).all()
    #
    #print 'frist', len(resres0)
    
    #for g in resres0:
      #print db().execute( ST_AsText(g) ).scalar()
      #print i.name, g.type
    #print 'second'
    #for i, g in res1:
      #print i.name, g.type

    
    #print 'disttest', db().query( ST_Distance( resres0, WKTElement('POINT(1.0 0.0 0.0)') ) ).scalar()

    # laeuft
   ## print 'disttest', db().query(ObjectInstance.id).filter( ST_Distance( ObjectInstance.tester(), WKTElement('POINT(1.0 0.0 0.0)') ) > 0).all()

    #print 'disttest', db().query(ObjectInstance.id).filter( ST_Distance( ObjectInstance.tester2(), WKTElement('POINT(1.0 0.0 0.0)') ) > 0).all()
    #print 'disttest', db().query(ObjectInstance.id).filter(ST_Distance( WKTElement('POINT(1.0 0.0 0.0)'), WKTElement('POINT(1.0 0.0 0.0)') ) > 0).all()
    #obj1.id, obj2.id).filter( db().execute( ST_DWithin( obj1.getAPosition2D() , obj2.getAPosition2D(), 20.0) ) ): #ST_DWithin( obj1.getAPosition2D() , obj2.getAPosition2D(), 2.0 )
    #dat = db().exists().where( db().execute( ST_Within( origin , ObjectInstance.getAPosition2D() ) ) )
    #obj_within_range = db().query(ObjectInstance).filter(ST_Within( origin , ObjectInstance.getAPosition2D) ).all()

   ## print 'disttest', db().query(ObjectInstance, ObjectInstance.frame).filter( ST_Distance( ObjectInstance.tester2(), WKTElement('POINT(1.0 0.0 0.0)') ) > 0).all()
    return GetObjectInstancesResponse()

def test_create_absolute_description(req):
    rospy.loginfo("SpatialDB SRVs: test_create_absolute_description")
    then = rospy.Time.now()
    res = GetObjectInstancesResponse()
    objects = db().query(ObjectInstance).filter(ObjectInstance.id.in_(req.ids)).all()

    for obj in objects:
      print "Create Absolute for", obj.name
      obj.createAbsoluteDescription()

    return res

def test_object_instances(req):
    rospy.loginfo("SpatialDB SRVs: test_object_instances")
    then = rospy.Time.now()
    res = GetObjectInstancesResponse()
    objects = db().query(ObjectInstance).filter(ObjectInstance.id.in_(req.ids)).all()

    for obj in objects:
      then2 = rospy.Time.now()

      #print 'coll',  obj.object_description.getGeometryCollection()
      #print 'coll aT', obj.object_description.getGeometryCollection( as_text = True )

      #print 'box', obj.object_description.getBoundingBox()
      #print 'box aT', obj.object_description.getBoundingBox( as_text = True )

      #print 'box2d', obj.object_description.getBox2D()
      #print 'box3d', obj.object_description.getBox3D()
      #print 'box3d mesh', box3DtoPolygonMesh( obj.object_description.getBox3D() )

      #print 'ch2d', obj.object_description.getConvexHull2D()
      #print 'ch2dt', obj.object_description.getConvexHull2D(True)

      pos2D = obj.getAPosition2D()
      pos3D = obj.getAPosition3D()
      print 'pos2D', db().execute( ST_AsText( obj.getAPosition2D() ) ).scalar()
      print 'pos3D', db().execute( ST_AsText( obj.getAPosition3D() ) ).scalar()
      #geom2D_1 = obj.getAConvexHull2D()
      geom2D_1 = origin
      #obj.getABox2D()
      #geom2D_2 = obj.getABox2D()
      geom2D_2 = obj.getAConvexHull2D()
      geom2D_2 = pos2D

      #geom3D_1 = obj.getAConvexHull3D()
      geom3D_1 = origin
      #geom3D_1 = origin
      #geom3D_2 = obj.getABox3D()
      #geom3D_2 = obj.getAConvexHull3D()
      geom3D_2 = pos3D

      print '## 2D ##'

      print '2d distance', db().execute( ST_Distance( geom2D_2 , geom2D_1) ).scalar()
      print '2d in range', db().execute( ST_DWithin( geom3D_1 , geom3D_2, 0.0) ).scalar()
      # no fully within 2D geometry fzunc
      #print '2d fully in range', db().execute( ST_DFullyWithin( geom3D_1 , geom3D_2, 4.0) ).scalar()

      print '2d within', db().execute( ST_Within( geom2D_1 , geom2D_2) ).scalar()
      print '2d contains', db().execute( ST_Contains( geom2D_1 , geom2D_2) ).scalar()

      print '2d touches', db().execute( ST_Touches( geom2D_1 , geom2D_2) ).scalar()
      print '2d intersects', db().execute( ST_Intersects( geom2D_1 , geom2D_2) ).scalar()
      print '2d disjoint', db().execute( ST_Disjoint( geom2D_1 , geom2D_2) ).scalar()

      print '## 3D ##'
      print 'in range', db().execute( ST_3DDWithin( geom3D_1 , geom3D_2, 0.0) ).scalar()
      print 'fully in range', db().execute( ST_3DDFullyWithin( geom3D_1 , geom3D_2, 4.0) ).scalar()

      print 'min distance', db().execute( ST_3DDistance( geom3D_1 , geom3D_2 ) ).scalar()
      print 'max distance', db().execute( ST_3DMaxDistance( geom3D_1 , geom3D_2 ) ).scalar()

      print 'min line', db().execute( ST_AsText( ST_3DShortestLine( geom3D_1 , geom3D_2 ) ) ).scalar()
      print 'max line', db().execute( ST_AsText( ST_3DLongestLine( geom3D_1 , geom3D_2 ) ) ).scalar()

      #print ' OTHER '
      #print 'skeleton', db().execute( ST_AsText( ST_StraightSkeleton( geom2D_1 ) ) ).scalar()
      #print 'skeleton', db().execute( ST_AsText( ST_StraightSkeleton( geom3D_1 ) ) ).scalar()

    rospy.loginfo("Get Test took %f seconds in total." % (rospy.Time.now() - then).to_sec())
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

