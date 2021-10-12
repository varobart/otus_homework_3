cmake_minimum_required(VERSION 3.9)

if(DEFINED ENV{GITHUB_RUN_NUMBER})
  set(PATCH_NUM $ENV{GITHUB_RUN_NUMBER})
else()
  set(PATCH_NUM 0)
endif()

project(async
  LANGUAGES CXX
  VERSION 0.0.${PATCH_NUM}
)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 17)

add_subdirectory(src)

#install(TARGETS async RUNTIME DESTINATION bin)
include(CPack)

