#include "src/include/rm.h"

namespace PeterDB {
    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }
}
