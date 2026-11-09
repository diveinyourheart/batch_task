cmake_minimum_required(VERSION 3.20)

project(
  batch_task
  VERSION 0.1.0
  DESCRIPTION "A single-threaded C++20 coroutine task scheduler"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "batch_task currently requires Linux epoll")
endif()

function(batch_task_warnings target)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
endfunction()

add_library(batch_task INTERFACE)
add_library(kaixyhe::batch_task ALIAS batch_task)
target_compile_features(batch_task INTERFACE cxx_std_20)
target_include_directories(
  batch_task
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

add_executable(batch_task_example examples/batch_task_example.cc)
target_link_libraries(batch_task_example PRIVATE kaixyhe::batch_task)
batch_task_warnings(batch_task_example)

include(CTest)

if(BUILD_TESTING)
  add_executable(batch_task_test tests/batch_task_test.cc)
  target_link_libraries(batch_task_test PRIVATE kaixyhe::batch_task)
  batch_task_warnings(batch_task_test)
  add_test(NAME batch_task_test COMMAND batch_task_test)
  set_tests_properties(batch_task_test PROPERTIES TIMEOUT 30)
endif()
