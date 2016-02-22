cmake_minimum_required(VERSION 2.8.3)
project(semap_ros)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  rospy
  roslib
  tf
  geometry_msgs
  shape_msgs
  visualization_msgs
  semap_msgs
  semap
  message_generation
)

add_service_files(DIRECTORY srv)

catkin_python_setup()

generate_messages(
DEPENDENCIES
  geometry_msgs
  shape_msgs
  visualization_msgs
  semap_msgs
)

catkin_package(
  INCLUDE_DIRS
  LIBRARIES
  CATKIN_DEPENDS
  rospy
  roscpp
  roslib
  tf
  geometry_msgs
  shape_msgs
  visualization_msgs
  semap_msgs
  semap
  message_runtime
  DEPENDS
)

include_directories(
  ${catkin_INCLUDE_DIRS}
)

install(DIRECTORY launch/ DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch)

catkin_install_python(PROGRAMS src/test/test_imports.py
 DESTINATION ${CATKIN_PACKAGE_PYTHON_DESTINATION})

catkin_install_python(PROGRAMS src/nodes/services.py
 DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION})
