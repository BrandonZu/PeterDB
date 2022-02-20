add_library(ix ix.cc IXFileHandle.cc IXSanIterator.cc IXPageHandle.cc IndexPageHandle.cc LeafPageHandle.cpp)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)