add_library(rm rm.cc RM_ScanIterator.cc CatalogTablesRecord.cc CatalogColumnsRecord.cc)
add_dependencies(rm rbfm ix googlelog)
target_link_libraries(rm rbfm ix glog)