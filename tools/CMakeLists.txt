add_executable(puller ${CMAKE_SOURCE_DIR}/tools/puller.cpp
                      ${CMAKE_SOURCE_DIR}/src/util.cpp)
target_link_libraries(puller ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(pusher ${CMAKE_SOURCE_DIR}/tools/pusher.cpp
                      ${CMAKE_SOURCE_DIR}/src/util.cpp
                      ${CMAKE_SOURCE_DIR}/src/frame_queue.cpp)
target_link_libraries(pusher ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(rtmp_press ${CMAKE_SOURCE_DIR}/tools/rtmp_press.cpp
                          ${CMAKE_SOURCE_DIR}/src/util.cpp
                          ${CMAKE_SOURCE_DIR}/src/checksum.cpp)
target_link_libraries(rtmp_press ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(random_test ${CMAKE_SOURCE_DIR}/tools/random_test.cpp
                           ${CMAKE_SOURCE_DIR}/src/util.cpp
                           ${CMAKE_SOURCE_DIR}/src/frame_queue.cpp)
target_link_libraries(random_test ${BRPC_LIB} ${DYNAMIC_LIB})

