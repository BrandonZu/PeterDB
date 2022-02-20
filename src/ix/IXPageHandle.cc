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
