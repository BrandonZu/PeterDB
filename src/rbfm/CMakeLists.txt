add_library(rbfm rbfm.cc RecordPageHandle.cc RecordHelper.cc)
add_dependencies(rbfm pfm googlelog)
target_link_libraries(rbfm pfm glog)