# Default LIBHDFSPP_DIR to the default install location.  You can override
#    it by add -DLIBHDFSPP_DIR=... to your cmake invocation
set(LIBHDFSPP_DIR CACHE STRING ${CMAKE_INSTALL_PREFIX})

include_directories( ${LIBHDFSPP_DIR}/include )
link_directories( ${LIBHDFSPP_DIR}/lib )

add_executable(declare declare.cc)
target_link_libraries(declare tools_common hdfspp_static)

add_executable(declare-segments declare-segments.cc)
target_link_libraries(declare-segments tools_common hdfspp_static)

add_executable(declare-segments-async declare-segments-async.cc)
target_link_libraries(declare-segments-async tools_common hdfspp_static)

add_executable(declare-transcode declare-transcode.cc)
target_link_libraries(declare-transcode tools_common hdfspp_static)

add_executable(declare-transcode-grouped declare-transcode-grouped.cc)
target_link_libraries(declare-transcode-grouped tools_common hdfspp_static)