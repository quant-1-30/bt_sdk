cmake_minimum_required(VERSION 3.14)
project(adj_factor)

set(PYBIND11_FINDPYTHON ON)
find_package(Python COMPONENTS Interpreter Development REQUIRED)

find_package(pybind11 REQUIRED)

pybind11_add_module(adj_factor src/factor.cpp pybind_factor.cpp)

target_include_directories(adj_factor PRIVATE ${CMAKE_SOURCE_DIR}/include)

set_target_properties(adj_factor PROPERTIES
    # 设置库的版本信息
    # VERSION 1.0.0
    # SOVERSION 1
    OUTPUT_NAME "adj_factor"
)

#设置输出目录
set_target_properties(adj_factor PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib
    LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib
    RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin
)

#  copy to system lib/ and include/
#install(TARGETS adj_factor
#        LIBRARY DESTINATION lib)
#
#install(DIRECTORY include/
#        DESTINATION include)
#