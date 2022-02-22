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
        root = 0;
    }

    IXFileHandle::~IXFileHandle() {
        flushMetaData();
        flushRoot();
    }

    RC IXFileHandle::open(const std::string& tmpFileName) {
        RC ret = 0;
        if(isOpen()) {
            return ERR_OPEN_FILE_ALREADY_OPEN;
        }

        fileName = tmpFileName;
        fs = new fstream(fileName, std::fstream::in | std::fstream::out | std::fstream::binary);
        if(!isOpen()) {
            return ERR_OPEN_FILE;
        }

        ret = readMetaData();
        if(ret) {
            return ret;
        }
        ret = readRoot();
        if(ret) {
            return ret;
        }
        return 0;
    }

    RC IXFileHandle::close() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        flushMetaData();
        flushRoot();
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
        fs->seekg((pageNum + IX::FILE_HIDDEN_PAGE_NUM + IX::FILE_ROOT_PAGE_NUM) * PAGE_SIZE, fs->beg);  // Page is 0-indexed
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
        fs->seekp((pageNum + IX::FILE_HIDDEN_PAGE_NUM + IX::FILE_ROOT_PAGE_NUM) * PAGE_SIZE, fs->beg);  // Page is 0-indexed
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
        fs->seekp(ixAppendPageCounter * PAGE_SIZE);
        fs->write((char *)data, PAGE_SIZE);
        fs->flush(); // IMPORTANT!
        if(!fs->good()) {
            return ERR_APPEND_PAGE;
        }
        ixAppendPageCounter++;
        flushMetaData();
        return 0;
    }

    RC IXFileHandle::appendEmptyPage() {
        uint8_t emptyPage[PAGE_SIZE];
        bzero(emptyPage, PAGE_SIZE);
        return appendPage(emptyPage);
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

    uint32_t IXFileHandle::getPageCounter() {
        return ixAppendPageCounter;     // Page won't be deleted -> appendCount = pageCounter
    }

    uint32_t IXFileHandle::getLastPageIndex() {
        return ixAppendPageCounter - 1;     // Page is 0-indexed
    }

    // Meta Data Format
    // Read | Write | Append
    RC IXFileHandle::readMetaData() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekg(fs->beg);
        fs->read((char *)(&ixReadPageCounter), IX::FILE_COUNTER_LEN);
        fs->read((char *)(&ixWritePageCounter), IX::FILE_COUNTER_LEN);
        fs->read((char *)(&ixAppendPageCounter), IX::FILE_COUNTER_LEN);
        return 0;
    }
    RC IXFileHandle::flushMetaData() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekp(fs->beg);
        fs->write((char *)(&ixReadPageCounter), IX::FILE_COUNTER_LEN);
        fs->write((char *)(&ixWritePageCounter), IX::FILE_COUNTER_LEN);
        fs->write((char *)(&ixAppendPageCounter), IX::FILE_COUNTER_LEN);
        return 0;
    }

    RC IXFileHandle::createRootPage() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        RC ret = 0;
        if(ixAppendPageCounter != IX::FILE_HIDDEN_PAGE_NUM) {
            return ERR_CREATE_ROOT;
        }
        ret = appendEmptyPage();
        if(ret) {
            return ret;
        }
        root = IX::FILE_ROOT_NULL;
        flushRoot();
        return 0;
    }
    RC IXFileHandle::readRoot() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekg(IX::FILE_HIDDEN_PAGE_NUM * PAGE_SIZE, fs->beg);  // Page 1 is Root Page
        fs->read((char *)(&root), IX::FILE_ROOT_LEN);
        fs->flush();
        return 0;
    }
    RC IXFileHandle::flushRoot() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekp(IX::FILE_HIDDEN_PAGE_NUM * PAGE_SIZE, fs->beg);  // Page 1 is Root Page
        fs->write((char *)(&root), IX::FILE_ROOT_LEN);
        fs->flush();
        return 0;
    }

    std::string IXFileHandle::getFileName() {
        return fileName;
    }

    bool IXFileHandle::isOpen() {
        return fs && fs->is_open();
    }

    bool IXFileHandle::isRootExist() {
        return root != IX::FILE_ROOT_NOT_EXIST;
    }

    bool IXFileHandle::isRootNull() {
        return root == IX::FILE_ROOT_NULL;  // Page 0 is the hidden page
    }
}

