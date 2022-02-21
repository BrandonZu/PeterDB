#include "src/include/ix.h"

namespace PeterDB {
    IXPageHandle::IXPageHandle(IXFileHandle& fileHandle, uint32_t page): fh(fileHandle), pageNum(page) {
        fh.readPage(pageNum, data);
        freeBytePtr = getFreeBytePointer();
        pageType = getPageType();
        counter = getCounter();
        parentPtr = getParentPtr();
    }

    IXPageHandle::IXPageHandle(IXPageHandle& pageHandle): fh(pageHandle.fh) {
        pageNum = pageHandle.pageNum;
        pageType = pageHandle.pageType;
        freeBytePtr = pageHandle.freeBytePtr;
        counter = pageHandle.counter;
        parentPtr = pageHandle.parentPtr;

        memcpy(data, pageHandle.data, PAGE_SIZE);
    }

    IXPageHandle::~IXPageHandle() {
        fh.writePage(pageNum, data);
    }

    bool IXPageHandle::isTypeIndex() {
        return pageType == IX::PAGE_TYPE_INDEX;
    }
    bool IXPageHandle::isTypeLeaf() {
        return pageType == IX::PAGE_TYPE_LEAF;
    }

    bool IXPageHandle::isKeyGreater(const uint8_t* key1, const uint8_t* key2, const Attribute& attr) {
        switch (attr.type) {
            case TypeInt:
                return *(int32_t*)key1 > *(int32_t*)key2;
                break;
            case TypeReal:
                return *(float*)key1 > *(float*)key2;
                break;
            case TypeVarChar:
                int len1, len2;
                memcpy(&len1, key1, sizeof(int32_t));
                memcpy(&len2, key2, sizeof(int32_t));
                std::string str1((char *)(key1 + sizeof(int32_t)), len1);
                std::string str2((char *)(key2 + sizeof(int32_t)), len2);
                return str1 > str2;
                break;
        }
        return false;
    }

    RC IXPageHandle::shiftRecordLeft(int16_t dataNeedShiftStartPos, int16_t dist) {
        int16_t dataNeedMoveLen = freeBytePtr - dataNeedShiftStartPos;
        if(dataNeedMoveLen < 0) {
            return ERR_IMPOSSIBLE;
        }
        if(dataNeedMoveLen == 0) {
            return 0;
        }

        // Must Use Memmove! Source and Destination May Overlap
        memmove(data + dataNeedShiftStartPos - dist, data + dataNeedShiftStartPos, dataNeedMoveLen);

        return 0;
    }

    RC IXPageHandle::shiftRecordRight(int16_t dataNeedShiftStartPos, int16_t dist) {
        int16_t dataNeedMoveLen = freeBytePtr - dataNeedShiftStartPos;
        if(dataNeedMoveLen < 0) {
            return ERR_IMPOSSIBLE;
        }
        if(dataNeedMoveLen == 0) {
            return 0;
        }
        // Must Use Memmove! Source and Destination May Overlap
        memmove(data + dataNeedShiftStartPos + dist, data + dataNeedShiftStartPos, dataNeedMoveLen);

        return 0;
    }

    int16_t IXPageHandle::getHeaderLen() {
        return IX::PAGE_TYPE_LEN + IX::PAGE_FREEBYTE_PTR_LEN + IX::PAGE_COUNTER_LEN + IX::PAGE_PARENT_PTR_LEN;
    }

    int16_t IXPageHandle::getPageType() {
        int16_t type;
        memcpy(&type, data + PAGE_SIZE - IX::PAGE_TYPE_LEN, IX::PAGE_TYPE_LEN);
        return type;
    }
    void IXPageHandle::setPageType(int16_t type) {
        memcpy(data + PAGE_SIZE - IX::PAGE_TYPE_LEN, &type, IX::PAGE_TYPE_LEN);
    }

    int16_t IXPageHandle::getFreeBytePointerOffset() {
        return PAGE_SIZE - IX::PAGE_TYPE_LEN - IX::PAGE_FREEBYTE_PTR_LEN;
    }
    int16_t IXPageHandle::getFreeBytePointer() {
        int16_t ptr;
        memcpy(&ptr, data + getFreeBytePointerOffset(), IX::PAGE_FREEBYTE_PTR_LEN);
        return ptr;
    }
    void IXPageHandle::setFreeBytePointer(int16_t ptr) {
        memcpy(data + getFreeBytePointerOffset(), &ptr, IX::PAGE_FREEBYTE_PTR_LEN);
    }

    int16_t IXPageHandle::getCounterOffset() {
        return PAGE_SIZE - IX::PAGE_TYPE_LEN - IX::PAGE_FREEBYTE_PTR_LEN - IX::PAGE_COUNTER_LEN;
    }
    int16_t IXPageHandle::getCounter() {
        int16_t counter;
        memcpy(&counter, data + getCounterOffset(), IX::PAGE_COUNTER_LEN);
        return counter;
    }
    void IXPageHandle::setCounter(int16_t counter) {
        memcpy(data + getCounterOffset(), &counter, IX::PAGE_COUNTER_LEN);
    }
    void IXPageHandle::addCounterByOne() {
        int16_t counter = getCounter();
        setCounter(counter + 1);
    }

    int16_t IXPageHandle::getParentPtrOffset() {
        return PAGE_SIZE - IX::PAGE_TYPE_LEN - IX::PAGE_FREEBYTE_PTR_LEN - IX::PAGE_COUNTER_LEN - IX::PAGE_PARENT_PTR_LEN;
    }
    uint32_t IXPageHandle::getParentPtr() {
        uint32_t parent;
        memcpy(&parent, data + getParentPtrOffset(), IX::PAGE_PARENT_PTR_LEN);
        return parent;
    }
    void IXPageHandle::setParentPtr(uint32_t parent) {
        memcpy(data + getParentPtrOffset(), &parent, IX::PAGE_PARENT_PTR_LEN);
    }

}
