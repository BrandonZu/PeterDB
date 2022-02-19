#include "src/include/ix.h"

namespace PeterDB {
    IXPageHandle::IXPageHandle(IXFileHandle& fileHandle, uint32_t page): fh(fileHandle), pageNum(page) {
        fh.readPage(pageNum, data);
        freeBytePtr = getFreeBytePointer();
        pageType = getPageType();
    }

    IXPageHandle::~IXPageHandle() {
        fh.writePage(pageNum, data);
    }

    bool IXPageHandle::isTypeIndex() {
        return pageType == IX::TYPE_INDEX_PAGE;
    }
    void IXPageHandle::setTypeIndex() {
        setPageType(IX::TYPE_INDEX_PAGE);
    }
    bool IXPageHandle::isTypeLeaf() {
        return pageType == IX::TYPE_LEAF_PAGE;
    }
    void IXPageHandle::setTypeLeaf() {
        setPageType(IX::TYPE_LEAF_PAGE);
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


}
