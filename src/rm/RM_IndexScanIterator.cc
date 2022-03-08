#include "src/include/rm.h"

using namespace PeterDB::RM;

namespace PeterDB {
    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        RC ret = ixIter.getNextEntry(rid, key);
        if(ret) {
            return RM_EOF;
        }
        return 0;
    }

    RC RM_IndexScanIterator::open(IXFileHandle* ixFileHandle, const Attribute& attr,
            const uint8_t* lowKey, const uint8_t* highKey,
            bool lowKeyInclusive, bool highKeyInclusive) {
        RC ret = 0;
        ret = ixIter.open(ixFileHandle, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
        if(ret) return ret;
        return 0;
    }

    RC RM_IndexScanIterator::close() {
        return ixIter.close();
    }
}


