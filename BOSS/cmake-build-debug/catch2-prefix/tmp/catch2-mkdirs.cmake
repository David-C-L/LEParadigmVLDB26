# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/catch2-prefix/src/catch2"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/catch2-prefix/src/catch2-build"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/catch2-prefix"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/catch2-prefix/tmp"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/catch2-prefix/src/catch2-stamp"
  "/home/davidl/.cmake-downloads/BOSS"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/catch2-prefix/src/catch2-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/catch2-prefix/src/catch2-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/catch2-prefix/src/catch2-stamp${cfgdir}") # cfgdir has leading slash
endif()
