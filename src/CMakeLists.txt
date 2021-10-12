#add_library(version hello.cpp)

#target_compile_features(version PUBLIC cxx_std_17)
#target_include_directories(version INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(libasync SHARED async.h async.cpp)

add_executable(async_cli main.cpp)

find_library(PTHREAD_LIBRARY pthread)
target_link_libraries(async_cli libasync ${PTHREAD_LIBRARY})
