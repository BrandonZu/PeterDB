#include "src/include/rm.h"

using namespace PeterDB::RM;

namespace PeterDB {
    RM_IndexScanIterator::RM_IndexScanIterator() {

    }

    RM_IndexScanIterator::~RM_IndexScanIterator() {

    }

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {

        return ERR_TODO;
    }

    RC RM_IndexScanIterator::close() {

        return ERR_TODO;
    }
}

