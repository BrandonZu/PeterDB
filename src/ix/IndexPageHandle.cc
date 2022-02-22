#include "src/include/ix.h"

namespace PeterDB {
    IndexPageHandle::IndexPageHandle(IXPageHandle& pageHandle): IXPageHandle(pageHandle) {

    }

    IndexPageHandle::IndexPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent): IXPageHandle(fileHandle, page) {
        setPageType(IX::PAGE_TYPE_INDEX);
        setCounter(0);
        setFreeBytePointer(0);
        setParentPtr(parent);
    }

    IndexPageHandle::~IndexPageHandle() {
        fh.writePage(pageNum, data);
    }

    RC IndexPageHandle::getTargetChild(uint32_t& childPtr, const uint8_t* key, const Attribute &attr) {
        RC ret = 0;
        int16_t pos = 0;
        if(counter == 0) {
            // Case 1: insert the first element

        }
        else if(isKeySatisfyComparison(key, data + pos + IX::INDEXPAGE_CHILD_PTR_LEN, attr, CompOp::LT_OP)) {
            // Case 2: new key is smallest

        }
        else {
            // Case 3: iterate over other elements
            for (int16_t index = 0; index < counter; index) {
                int16_t curKeyLen = getKeyLen(data + pos + IX::INDEXPAGE_CHILD_PTR_LEN, attr);
                if (isKeySatisfyComparison(key, data + pos + IX::INDEXPAGE_CHILD_PTR_LEN, attr, CompOp::GE_OP)) {

                }
            }
        }
        return 0;
    }

    RC IndexPageHandle::insertIndex() {

    }

    RC IndexPageHandle::print(const Attribute &attr, std::ostream &out) {
        RC ret = 0;
        // 1. Keys
        out << "{\"keys\": [";
        std::queue<int> children;
        int16_t offset = 0;
        uint32_t child;
        for(int16_t i = 0; i < counter; i++) {
            memcpy(&child, data + offset, IX::INDEXPAGE_CHILD_PTR_LEN);
            children.push(child);
            offset += IX::INDEXPAGE_CHILD_PTR_LEN;
            int16_t keyLen = getKeyLen(data + offset, attr);
            out << "\"";
            switch (attr.type) {
                case TypeInt:
                    out << getKeyInt(data + offset);
                    break;
                case TypeReal:
                    out << getKeyReal(data + offset);
                    break;
                case TypeVarChar:
                    out << getKeyString(data + offset);
                    break;
                default:
                    return ERR_KEY_TYPE_NOT_SUPPORT;
            }
            offset += keyLen;
            out << "\"";
            if(i != counter - 1) {
                out << ",";
            }
        }
        // Last Child if exist
        memcpy(&child, data + offset, IX::INDEXPAGE_CHILD_PTR_LEN);
        if(child >= IX::FILE_HIDDEN_PAGE_NUM + IX::FILE_ROOT_PAGE_NUM) {
            children.push(child);
        }
        out << "]," << std::endl;
        // 2. Children
        out << "\"children\": [" << std::endl;
        while(!children.empty()) {
            IXPageHandle pageHandle(fh, children.front());
            if(pageHandle.isTypeIndex()) {
                IndexPageHandle indexPH(pageHandle);
                indexPH.print(attr, out);
            }
            else if(pageHandle.isTypeLeaf()) {
                LeafPageHandle leafPH(pageHandle);
                leafPH.print(attr, out);
            }
            children.pop();
            if(!children.empty()) {
                out << ",";
            }
            out << std::endl;
        }
        out << "]}";
        return 0;
    }


    bool hasEnoughSpace(const uint8_t* key, const Attribute &attr) {

    }

    int16_t IndexPageHandle::getIndexHeaderLen() {
        return getHeaderLen();
    }
    int16_t IndexPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePtr - getIndexHeaderLen();
    }

}
