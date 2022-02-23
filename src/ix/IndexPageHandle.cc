#include "src/include/ix.h"

namespace PeterDB {
    IndexPageHandle::IndexPageHandle(IXPageHandle& pageHandle): IXPageHandle(pageHandle) {

    }

    IndexPageHandle::IndexPageHandle(IXFileHandle& fileHandle, uint32_t page): IXPageHandle(fileHandle, page) {

    }

//    IndexPageHandle::IndexPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent): IXPageHandle(fileHandle, page) {
//        setPageType(IX::PAGE_TYPE_INDEX);
//        setCounter(0);
//        setFreeBytePointer(0);
//        setParentPtr(parent);
//    }

    // Initialize the new page with one key
    IndexPageHandle::IndexPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent,
                                     uint32_t leftPage, uint8_t* key, uint32_t rightPage, const Attribute &attr): IXPageHandle(fileHandle, page) {
        setPageType(IX::PAGE_TYPE_INDEX);
        setCounter(1);
        setParentPtr(parent);

        int16_t pos = 0;
        // Write left page pointer
        memcpy(data + pos, &leftPage, IX::INDEXPAGE_CHILD_PTR_LEN);
        pos += IX::INDEXPAGE_CHILD_PTR_LEN;
        // Write the key
        int16_t keyLen = getKeyLen(key, attr);
        memcpy(data + pos, key, keyLen);
        pos += keyLen;
        // Write right page pointer
        memcpy(data + pos, &rightPage, IX::INDEXPAGE_CHILD_PTR_LEN);
        pos += IX::INDEXPAGE_CHILD_PTR_LEN;

        setFreeBytePointer(pos);
    }

    IndexPageHandle::~IndexPageHandle() {
        flushIndexHeader();
        fh.writePage(pageNum, data);
    }

    RC IndexPageHandle::getTargetChild(uint32_t& childPtr, const uint8_t* key, const Attribute &attr) {
        RC ret = 0;
        int16_t pos = 0;
        ret = findPosToInsertKey(pos, key, attr);
        if(ret) return ret;
        // Get previous child pointer
        pos -= IX::INDEXPAGE_CHILD_PTR_LEN;
        memcpy(&childPtr, data + pos, IX::INDEXPAGE_CHILD_PTR_LEN);
        return 0;
    }

    RC IndexPageHandle::findPosToInsertKey(int16_t& keyPos, const uint8_t* key, const Attribute& attr) {
        if(counter == 0) {
            return ERR_INDEXPAGE_NO_INDEX;
        }
        // 1. Smallest Key
        keyPos = IX::INDEXPAGE_CHILD_PTR_LEN;
        if(isKeySatisfyComparison(key, data + keyPos, attr, CompOp::LT_OP)) {
            return 0;
        }

        // 2. Iterate over all following keys
        keyPos += getEntryLen(data + keyPos, attr);
        int16_t prevPos = IX::INDEXPAGE_CHILD_PTR_LEN;
        for(int16_t i = 1; i < counter; i++) {
            if(isKeySatisfyComparison(key, data + prevPos, attr, CompOp::GE_OP) &&
               isKeySatisfyComparison(key, data + keyPos, attr, CompOp::LT_OP)) {
                break;
            }
            prevPos = keyPos;
            keyPos += getEntryLen(data + keyPos, attr);
        }

        return 0;
    }

    RC IndexPageHandle::insertIndex(const uint8_t* key, const Attribute& attr, uint32_t newChildPtr) {
        RC ret = 0;
        if(hasEnoughSpace(key, attr)) {
            ret = insertIndexWithEnoughSpace(key, attr, newChildPtr);
            if(ret) return ret;
        }
        else {
            // Not enough space, need to split
            uint32_t newIndexNum;
            ret = splitPageAndInsertIndex(newIndexNum, key, attr, newChildPtr);
            if(ret) return ret;

            if(parentPtr == IX::PAGE_PTR_NULL) {
                // No parent ptr, need to add one
                
            }
            else {
                IndexPageHandle parentPH(fh, parentPtr);
                parentPH.insertIndex(key, attr, newIndexNum);
            }
        }
        return 0;
    }

    RC IndexPageHandle::insertIndexWithEnoughSpace(const uint8_t* key, const Attribute& attr, uint32_t newPageNum) {
        RC ret = 0;
        int16_t insertPos = 0;
        ret = findPosToInsertKey(insertPos, key, attr);
        if(ret) return ret;
        // Shift following data right
        if(insertPos > freeBytePtr) {
            return ERR_PTR_BEYONG_FREEBYTE;
        }
        int16_t newEntryLen = getEntryLen(key, attr);
        if(insertPos < freeBytePtr) {
            // Insert Entry in the middle, Need to shift entries right
            ret = shiftRecordRight(insertPos, newEntryLen);
            if(ret) {
                return ret;
            }
        }
        ret = writeIndex(insertPos, key, attr, newPageNum);
        if(ret) return ret;
        // Update Free Byte Ptr
        setFreeBytePointer(freeBytePtr + newEntryLen);
        // Update Counter
        addCounterByOne();
        return 0;
    }

    RC IndexPageHandle::writeIndex(int16_t pos, const uint8_t* key, const Attribute& attr, uint32_t newPageNum) {
        RC ret = 0;
        int keyLen = getKeyLen(key, attr);
        memcpy(data + pos, key, keyLen);
        pos += keyLen;
        memcpy(data + pos, &newPageNum, IX::INDEXPAGE_CHILD_PTR_LEN);
        return 0;
    }

    // TODO
    RC IndexPageHandle::splitPageAndInsertIndex(uint32_t & newIndexNum, const uint8_t* key, const Attribute& attr, uint32_t newChildPtr) {
        RC ret = 0;
        // 0. Append a new page
        ret = fh.appendEmptyPage();
        if(ret) return ret;
        uint32_t newIndexPtr = fh.getLastPageIndex();

        return 0;
    }

    int16_t IndexPageHandle::getEntryLen(const uint8_t* key, const Attribute& attr) {
        return getKeyLen(key, attr) + IX::INDEXPAGE_CHILD_PTR_LEN;
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


    bool IndexPageHandle::hasEnoughSpace(const uint8_t* key, const Attribute &attr) {
        return getFreeSpace() >= getKeyLen(key, attr) + IX::INDEXPAGE_CHILD_PTR_LEN;
    }

    int16_t IndexPageHandle::getIndexHeaderLen() {
        return getHeaderLen();
    }

    void IndexPageHandle::flushIndexHeader() {
        flushHeader();
    }

    int16_t IndexPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePtr - getIndexHeaderLen();
    }

}
