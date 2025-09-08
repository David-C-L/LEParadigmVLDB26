cmake_minimum_required(VERSION 3.10)
project(BOSSParquetReaderEngine)

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

# Set Arrow and Parquet versions
set(ARROW_VERSION 7.0.0)

# Set the install directory for Arrow and Parquet
set(ARROW_INSTALL_DIR ${CMAKE_BINARY_DIR}/arrow-install)

# External project to build both Arrow and Parquet (since they are in the same repo)
ExternalProject_Add(arrow_project
    PREFIX ${CMAKE_BINARY_DIR}/arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG apache-arrow-${ARROW_VERSION}
    SOURCE_SUBDIR cpp
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}
        -DARROW_PARQUET=ON         
        -DARROW_CSV=ON             
        -DARROW_FILESYSTEM=ON      
        -DARROW_BUILD_SHARED=ON    
        -DARROW_BUILD_TESTS=OFF
	-DARROW_WITH_ZLIB=ON
        -DARROW_WITH_BZ2=ON
        -DARROW_WITH_ZLIB=ON
        -DARROW_WITH_SNAPPY=ON
        -DARROW_WITH_LZ4=ON
        -DARROW_WITH_ZSTD=ON
        -DARROW_WITH_IO=ON
	-DARROW_JEMALLOC=OFF
    BUILD_ALWAYS ON
    INSTALL_COMMAND make install
)

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
		  CMAKE_ARGS -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE} -DCMAKE_INSTALL_PREFIX=${BOSSParquetReaderEngine_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
                 -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
      BUILD_BYPRODUCTS ${BOSSParquetReaderEngine_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX}
    )

  ########################## Other Options and Library Dependencies ############################

  ExternalProject_Add(catch2
    URL https://github.com/catchorg/Catch2/archive/v2.13.6.tar.gz
    DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSSParquetReaderEngine_BINARY_DIR}/deps -DCATCH_BUILD_TESTING=NO -DBUILD_TESTING=NO -DCATCH_ENABLE_WERROR=NO -DCATCH_INSTALL_DOCS=NO -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    )
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_package(Boost REQUIRED)
  
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

set(ImplementationFiles Source/BOSSParquetReaderEngine.cpp)
set(TestFiles Tests/BOSSParquetReaderEngineTests.cpp)

add_library(BOSSParquetReaderEngine MODULE ${ImplementationFiles})

add_executable(PRTests ${ImplementationFiles} ${TestFiles})
add_dependencies(PRTests catch2)

set_property(TARGET BOSSParquetReaderEngine PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20
set_property(TARGET PRTests PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20

target_include_directories(BOSSParquetReaderEngine SYSTEM PUBLIC ${BOSSParquetReaderEngine_BINARY_DIR}/deps/include ${ARROW_INSTALL_DIR}/include ${Boost_INCLUDE_DIRS})
target_include_directories(PRTests SYSTEM PUBLIC ${BOSSParquetReaderEngine_BINARY_DIR}/deps/include ${ARROW_INSTALL_DIR}/include ${Boost_INCLUDE_DIRS})

target_link_directories(BOSSParquetReaderEngine PUBLIC ${BOSSParquetReaderEngine_BINARY_DIR}/deps/lib ${ARROW_INSTALL_DIR}/lib)
target_link_directories(PRTests PUBLIC ${BOSSParquetReaderEngine_BINARY_DIR}/deps/lib  ${ARROW_INSTALL_DIR}/lib)
add_dependencies(BOSSParquetReaderEngine BOSS arrow_project)
add_dependencies(PRTests BOSS arrow_project)

target_link_libraries(PRTests ${CMAKE_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX} arrow parquet ${Boost_LIBRARIES})

set(PUBLIC_HEADER_LIST
  ${CMAKE_CURRENT_SOURCE_DIR}/Source/BOSSParquetReaderEngine.hpp;
  )


list(APPEND AllTargets BOSSParquetReaderEngine)

foreach(Target IN LISTS AllTargets)
  if(NOT WIN32)
    target_link_libraries(${Target} dl)
  endif(NOT WIN32)
    target_link_libraries(${Target} Threads::Threads)
    set_target_properties(${Target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    if(MSVC)
        target_compile_options(${Target} PUBLIC "/Zc:__cplusplus")
        target_compile_options(${Target} PUBLIC "/EHsc")
    endif()
    set_target_properties(${Target} PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
    add_dependencies(${Target} BOSS arrow_project)
    target_include_directories(${Target} SYSTEM PUBLIC ${BOSSParquetReaderEngine_BINARY_DIR}/deps/include)
    target_include_directories(${Target} SYSTEM PUBLIC ${CMAKE_BINARY_DIR}/deps/include)
    target_link_libraries(${Target} ${CMAKE_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX} arrow parquet ${Boost_LIBRARIES})
    if(UNIX AND NOT APPLE)
        target_link_libraries(${Target} rt)
    endif()
endforeach()

set_target_properties(BOSSParquetReaderEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
set_target_properties(BOSSParquetReaderEngine PROPERTIES PUBLIC_HEADER ${PUBLIC_HEADER_LIST})
install(TARGETS BOSSParquetReaderEngine LIBRARY DESTINATION lib)

set_target_properties(PRTests PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS PRTests RUNTIME DESTINATION bin)
