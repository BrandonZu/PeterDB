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

    int16_t IndexPageHandle::getIndexHeaderLen() {
        return getHeaderLen();
    }
    int16_t IndexPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePtr - getIndexHeaderLen();
    }

    RC IndexPageHandle::insertIndex() {
        for(uint16_t i = 0; i < counter; i++) {

        }
    }

}
