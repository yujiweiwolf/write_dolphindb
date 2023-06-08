# cmake_minimum_required(VERSION <specify CMake version here>)
cmake_minimum_required(VERSION 3.19)
project(write_dolphindb)

#Release Debug
set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall")
add_compile_options(-std=c++17 -Wall -O2)
#add_definitions(-DLOGGING_LEVEL_2 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=0)
#add_definitions(-DLOGGING_LEVEL_2 -DLINUX)
add_definitions(-DLOGGING_LEVEL_2 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=1)

set(ROOT_DIR "/home/work/sys/lib")
macro(SUBDIRLIST result curdir)
    file(GLOB children RELATIVE ${curdir} ${curdir}/*)
    set(dirlist "")
    foreach(child ${children})
        if(IS_DIRECTORY ${curdir}/${child})
            LIST(APPEND dirlist ${child})
        endif()
    endforeach()
    set(${result} ${dirlist})
endmacro()

SUBDIRLIST(all_sub_index ${ROOT_DIR})
foreach(child ${all_sub_index})
    list(APPEND all_include ${ROOT_DIR}/${child}/include)
    list(APPEND all_lib ${ROOT_DIR}/${child}/lib)
endforeach()

include_directories(${CMAKE_CURRENT_LIST_DIR}/lib/dolphindb/include lib/openssl-1.0.2u/include ${all_include})
link_directories(${CMAKE_CURRENT_LIST_DIR}/lib/dolphindb/lib lib/openssl-1.0.2u  ${all_lib})

set(DOLPHINDB_TEST "dolphindb_test")

# test_dolphindb
add_executable(${DOLPHINDB_TEST} src/test_dolphindb/test_dolphidb.cc)
target_link_libraries(${DOLPHINDB_TEST}
        libDolphinDBAPI.so broker coral swordfish x stdc++fs yaml-cpp  clickhouse-cpp-lib-static boost_date_time boost_filesystem boost_regex boost_system  boost_chrono boost_log boost_program_options boost_thread boost_iostreams z protobuf protobuf-lite sodium zmq ssl crypto iconv pthread dl)


FILE(GLOB LIBS_NAME "lib/openssl-1.0.2u/*.so*")
FILE(COPY ${LIBS_NAME} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
FILE(COPY lib/dolphindb/lib/libDolphinDBAPI.so DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

FILE(GLOB COMMON_LIB_NAME "${ROOT_DIR}/common_lib/lib/*so*")
FILE(COPY ${COMMON_LIB_NAME} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

