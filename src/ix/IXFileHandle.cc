#include "src/include/ix.h"

using std::ios;
using std::fstream;
using std::ifstream;
using std::ofstream;

namespace PeterDB {
    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        fs = nullptr;
    }

    IXFileHandle::~IXFileHandle() {
        flushMetaData();
    }

    RC IXFileHandle::open(const std::string& tmpFileName) {
        if(isOpen()) {
            return ERR_OPEN_FILE_ALREADY_OPEN;
        }

        fileName = tmpFileName;
        fs = new fstream(fileName, std::fstream::in | std::fstream::out | std::fstream::binary);
        if(!isOpen()) {
            return ERR_OPEN_FILE;
        }

        RC rc = readMetaData();
        if(rc) {
            return rc;
        }
        return 0;
    }

    RC IXFileHandle::close() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        flushMetaData();

        delete fs;
        fs = nullptr;
        return 0;
    }

    RC IXFileHandle::readPage(uint32_t pageNum, void* data) {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        if(pageNum >= ixAppendPageCounter) {
            return ERR_PAGE_NOT_EXIST;
        }

        fs->clear();
        fs->seekg((pageNum + 1) * PAGE_SIZE, fs->beg);  // Page is 0-indexed, Page 0 is hidden page
        fs->read((char *)data, PAGE_SIZE);
        if(!fs->good()) {
            return ERR_READ_PAGE;
        }
        ixReadPageCounter++;
        flushMetaData();
        return 0;
    }

    RC IXFileHandle::writePage(uint32_t pageNum, const void* data) {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        if(pageNum >= ixAppendPageCounter) {
            return ERR_PAGE_NOT_EXIST;
        }

        fs->clear();
        fs->seekp((pageNum + 1) * PAGE_SIZE, fs->beg);  // Page is 0-indexed, Page 0 is hidden page
        fs->write((char *)data, PAGE_SIZE);
        fs->flush(); // IMPORTANT!
        if(!fs->good()) {
            return ERR_WRITE_PAGE;
        }
        ixWritePageCounter++;
        flushMetaData();
        return 0;
    }

    RC IXFileHandle::appendPage(const void* data) {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }

        fs->clear();
        fs->seekp((ixAppendPageCounter + 1) * PAGE_SIZE);
        fs->write((char *)data, PAGE_SIZE);
        fs->flush(); // IMPORTANT!
        if(!fs->good()) {
            return ERR_APPEND_PAGE;
        }
        ixAppendPageCounter++;
        flushMetaData();
        return 0;
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

    // Meta Data Format
    // Read | Write | Append | Root | Type
    RC IXFileHandle::readMetaData() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekg(fs->beg);
        // ReadPageCounter
        fs->read((char *)(&ixReadPageCounter), IX::FILE_COUNTER_LEN);
        // WritePageCounter
        fs->read((char *)(&ixWritePageCounter), IX::FILE_COUNTER_LEN);
        // AppendPageCounter
        fs->read((char *)(&ixAppendPageCounter), IX::FILE_COUNTER_LEN);
        // Root
        fs->read((char *)(&root), IX::FILE_ROOT_LEN);
        // Type
        fs->read((char *)(&keyType), IX::FILE_TYPE_LEN);
        return 0;
    }

    RC IXFileHandle::flushMetaData() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekp(fs->beg);
        // ReadPageCounter
        fs->write((char *)(&ixReadPageCounter), IX::FILE_COUNTER_LEN);
        // WritePageCounter
        fs->write((char *)(&ixWritePageCounter), IX::FILE_COUNTER_LEN);
        // AppendPageCounter
        fs->write((char *)(&ixAppendPageCounter), IX::FILE_COUNTER_LEN);
        // Root
        fs->write((char *)(&root), IX::FILE_ROOT_LEN);
        // Type
        fs->write((char *)(&keyType), IX::FILE_TYPE_LEN);
        return 0;
    }

    std::string IXFileHandle::getFileName() {
        return fileName;
    }

    bool IXFileHandle::isOpen() {
        return fs && fs->is_open();
    }

    bool IXFileHandle::isRootNull() {
        return root == IX::FILE_ROOT_NULL;  // Page 0 is the hidden page
    }
}

