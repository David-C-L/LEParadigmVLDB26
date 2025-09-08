cmake_minimum_required(VERSION 3.10)
project(BOSSRemoteBinaryLoaderEngine)

############################## Custom build modes ###############################

set(CMAKE_CXX_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C++ compiler during Sanitize builds."
  FORCE)
set(CMAKE_C_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C compiler during Sanitize builds."
  FORCE)
set(CMAKE_EXE_LINKER_FLAGS_SANITIZE
  ${CMAKE_EXE_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used for linking binaries during Sanitize builds."
  FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  ${CMAKE_SHARED_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used by the shared libraries linker during Sanitize builds."
  FORCE)
mark_as_advanced(
  CMAKE_CXX_FLAGS_SANITIZE		  CMAKE_EXE_LINKER_FLAGS_SANITIZE
  CMAKE_C_FLAGS_SANITIZE		  CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  )

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
  "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
  FORCE)

############################### External Projects ###############################

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

if(MSVC)
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_LINK_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_LINK_LIBRARY_SUFFIX})
else()
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(MSVC)

include(ExternalProject)

# Core BOSS
  if(NOT DEFINED BOSS_SOURCE_REPOSITORY)
    if(NOT GITHUB_TOKEN)
      message( FATAL_ERROR "You need to pass -DGITHUB_TOKEN=... to pass a github token to fetch the boss release or -DBOSS_SOURCE_REPOSITORY= for a non-github clone" )
    endif()
    set(BOSS_SOURCE_REPOSITORY https://little-big-h:${GITHUB_TOKEN}@github.com/symbol-store/BOSS.git)
  endif()
  if(NOT DEFINED BOSS_SOURCE_BRANCH)
    set(BOSS_SOURCE_BRANCH temp_lazy_loading_compression)
  endif()
  message("getting boss sources from ${BOSS_SOURCE_REPOSITORY}, branch ${BOSS_SOURCE_BRANCH}")
  ExternalProject_Add(BOSS
      GIT_REPOSITORY ${BOSS_SOURCE_REPOSITORY}
      GIT_TAG ${BOSS_SOURCE_BRANCH}
      GIT_SHALLOW ON
		  CMAKE_ARGS -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE} -DCMAKE_INSTALL_PREFIX=${BOSSRemoteBinaryLoaderEngine_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
                 -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
      BUILD_BYPRODUCTS ${BOSSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX}
      )
#
    

########################## Other Options and Library Dependencies ############################

  set(CURL_INCLUDE_DIR /data/david/curl_nodup/temp_libs/include)
  set(CURL_LIBRARIES /data/david/curl_nodup/temp_libs/lib)
  set(CURL_LIB /data/david/curl_nodup/temp_libs/lib/libcurl.so.4)

  ExternalProject_Add(catch2
	URL https://github.com/catchorg/Catch2/archive/v2.13.6.tar.gz
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
	CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSSRemoteBinaryLoaderEngine_BINARY_DIR}/deps -DCATCH_BUILD_TESTING=NO -DBUILD_TESTING=NO -DCATCH_ENABLE_WERROR=NO -DCATCH_INSTALL_DOCS=NO -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
	)
      
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
  
#################################### Targets ####################################

if(MSVC)
  # not making any difference on Windows
  # and helps Visual Studio to parse it correctly as a target
  set(LibraryType SHARED)
else()
  set(LibraryType MODULE)
endif(MSVC)

if(MSVC)
  set(pluginInstallDir bin)
else()
  set(pluginInstallDir lib)
endif(MSVC)

set(ImplementationFiles Source/BOSSRemoteBinaryLoaderEngine.cpp)
set(TestFiles Tests/BOSSRemoteBinaryLoaderTests.cpp)

add_library(BOSSRemoteBinaryLoaderEngine MODULE ${ImplementationFiles})

add_executable(RBLTests ${ImplementationFiles} ${TestFiles})
add_dependencies(RBLTests catch2)
add_dependencies(BOSSRemoteBinaryLoaderEngine BOSS)
add_dependencies(RBLTests BOSS)

set_property(TARGET BOSSRemoteBinaryLoaderEngine PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20
set_property(TARGET RBLTests PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20

target_include_directories(BOSSRemoteBinaryLoaderEngine SYSTEM PUBLIC ${BOSSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/include ${CMAKE_BINARY_DIR}/deps/include ${CURL_INCLUDE_DIR})
target_include_directories(RBLTests SYSTEM PUBLIC ${BOSSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/include ${CURL_INCLUDE_DIR})

target_link_directories(BOSSRemoteBinaryLoaderEngine PUBLIC ${BOSSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/lib ${CURL_LIBRARIES})
target_link_directories(RBLTests PUBLIC ${BOSSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/lib ${CURL_LIBRARIES})

include_directories(${CURL_INCLUDE_DIR})
target_link_libraries(BOSSRemoteBinaryLoaderEngine ${CMAKE_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX} Threads::Threads dl rt ${CURL_LIB})
target_link_libraries(RBLTests Threads::Threads dl rt ${CURL_LIB})

set(PUBLIC_HEADER_LIST
  ${CMAKE_CURRENT_SOURCE_DIR}/Source/BOSSRemoteBinaryLoaderEngine.hpp
  )

set_target_properties(BOSSRemoteBinaryLoaderEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
set_target_properties(BOSSRemoteBinaryLoaderEngine PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(BOSSRemoteBinaryLoaderEngine PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
set_target_properties(BOSSRemoteBinaryLoaderEngine PROPERTIES PUBLIC_HEADER ${PUBLIC_HEADER_LIST})
install(TARGETS BOSSRemoteBinaryLoaderEngine LIBRARY DESTINATION lib)

set_target_properties(RBLTests PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS RBLTests RUNTIME DESTINATION bin)
