#include "src/include/ix.h"

namespace PeterDB {
    IndexPageHandle::IndexPageHandle(IXPageHandle& pageHandle): IXPageHandle(pageHandle) {

    }

    IndexPageHandle::~IndexPageHandle() = default;

    int16_t IndexPageHandle::getIndexHeaderLen() {
        return getHeaderLen();
    }
    int16_t IndexPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePtr - getIndexHeaderLen();
    }

    

}
