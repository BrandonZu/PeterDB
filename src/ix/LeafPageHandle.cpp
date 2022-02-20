#include "src/include/ix.h"

namespace PeterDB {
    LeafPageHandle::LeafPageHandle(IXPageHandle& pageHandle): IXPageHandle(pageHandle) {
        nextPtr = getNextPtr();
    }
    LeafPageHandle::~LeafPageHandle() {

    }

    int16_t LeafPageHandle::getLeafHeaderLen() {
        return getHeaderLen() + IX::LEAFPAGE_NEXT_PTR_LEN;
    }
    int16_t LeafPageHandle::getFreeSpace() {
        return PAGE_SIZE - getLeafHeaderLen() - freeBytePtr;
    }

    int16_t LeafPageHandle::getNextPtrOffset() {
        return PAGE_SIZE - getHeaderLen() - IX::LEAFPAGE_NEXT_PTR_LEN;
    }
    uint32_t LeafPageHandle::getNextPtr() {
        uint32_t nextPtr;
        memcpy(&nextPtr, data + getNextPtrOffset(), IX::LEAFPAGE_NEXT_PTR_LEN);
        return nextPtr;
    }
    void LeafPageHandle::setNextPtr(uint32_t next) {
        memcpy(data + getNextPtrOffset(), &nextPtr, IX::LEAFPAGE_NEXT_PTR_LEN);
    }
}
