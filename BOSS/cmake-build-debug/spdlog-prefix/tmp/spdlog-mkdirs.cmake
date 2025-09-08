# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/spdlog-prefix/src/spdlog"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/spdlog-prefix/src/spdlog-build"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/spdlog-prefix"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/spdlog-prefix/tmp"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/spdlog-prefix/src/spdlog-stamp"
  "/home/davidl/.cmake-downloads/BOSS"
  "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/spdlog-prefix/src/spdlog-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/spdlog-prefix/src/spdlog-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/davidl/Documents/PhD/symbol-store/BOSS/cmake-build-debug/spdlog-prefix/src/spdlog-stamp${cfgdir}") # cfgdir has leading slash
endif()
