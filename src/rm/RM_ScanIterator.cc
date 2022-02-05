#include "src/include/rm.h"

namespace PeterDB {
    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::open(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute, const CompOp compOp, const void *value,
            const std::vector<std::string> &attributeNames) {
        RC ret;
        ret = rbfmIter.open(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
        if(ret) {
            LOG(ERROR) << "Fail to open RM scan iterator @ RM_ScanIterator::open" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        RC ret;
        ret = rbfmIter.getNextRecord(rid, data);
        if(ret)
            return RM_EOF;
        else
            return 0;
    }

    RC RM_ScanIterator::close() {
        return rbfmIter.close();
    }
}
