#include "src/include/ix.h"

namespace PeterDB {
    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = fh.readPageCounter;
        ixWritePageCounter = fh.writePageCounter;
        ixAppendPageCounter = fh.appendPageCounter;
    }

    IXFileHandle::~IXFileHandle() = default;

    RC IXFileHandle::open(const std::string& filename) {
        return fh.open(filename);
    }

    RC IXFileHandle::close() {
        return fh.close();
    }

    RC IXFileHandle::readPage(uint32_t pageNum, void* data) {
        RC ret = fh.readPage(pageNum, data);
        if(ret) {
            return ret;
        }
        ixReadPageCounter = fh.readPageCounter;
        return 0;
    }

    RC IXFileHandle::writePage(uint32_t pageNum, const void* data) {
        RC ret = fh.writePage(pageNum, data);
        if(ret) {
            return ret;
        }
        ixWritePageCounter = fh.writePageCounter;
        return 0;
    }

    RC IXFileHandle::appendPage(const void* data) {
        RC ret = fh.appendPage(data);
        if(ret) {
            return ret;
        }
        ixAppendPageCounter = fh.appendPageCounter;
        return 0;
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

    std::string IXFileHandle::getFileName() {
        return fh.fileName;
    }
}

