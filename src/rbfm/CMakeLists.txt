add_library(rbfm rbfm.cc RecordPageHandle.cc RecordHelper.cc ApiDataHelper.cc RBFM_ScanIterator.cc)
add_dependencies(rbfm pfm googlelog)
target_link_libraries(rbfm pfm glog)