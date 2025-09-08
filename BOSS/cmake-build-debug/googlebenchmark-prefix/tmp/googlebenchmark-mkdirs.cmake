# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/googlebenchmark-prefix/src/googlebenchmark"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/googlebenchmark-prefix/src/googlebenchmark-build"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/googlebenchmark-prefix"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/googlebenchmark-prefix/tmp"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/googlebenchmark-prefix/src/googlebenchmark-stamp"
  "/home/davidl/.cmake-downloads/BOSS"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/googlebenchmark-prefix/src/googlebenchmark-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/googlebenchmark-prefix/src/googlebenchmark-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/googlebenchmark-prefix/src/googlebenchmark-stamp${cfgdir}") # cfgdir has leading slash
endif()
