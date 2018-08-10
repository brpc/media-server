protobuf_generate_cpp(PROTO_SRC PROTO_HEADER
                      ${CMAKE_SOURCE_DIR}/src/media_server.proto)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_executable(media_server ${SOURCES} ${PROTO_SRC})
target_link_libraries(media_server ${BRPC_LIB} ${DYNAMIC_LIB})
