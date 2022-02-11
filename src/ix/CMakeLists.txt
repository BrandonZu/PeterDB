add_library(ix ix.cc IXPageHandle.cc IXSanIterator.cc)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)