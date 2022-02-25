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
        rootPagePtr = IX::PAGE_PTR_NULL;
        root = IX::PAGE_PTR_NULL;
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
        fs->seekg(pageNum * PAGE_SIZE, fs->beg);  // Page is 0-indexed
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
        fs->seekp(pageNum * PAGE_SIZE, fs->beg);  // Page is 0-indexed
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
        return ixAppendPageCounter - 1;     // Page is 0-indexed, page 0: hidden page; page 1: root page
    }

    // Meta Data Format
    // Read | Write | Append | RootPage
    RC IXFileHandle::readMetaData() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekg(fs->beg);
        fs->read((char *)(&ixReadPageCounter), IX::FILE_COUNTER_LEN);
        fs->read((char *)(&ixWritePageCounter), IX::FILE_COUNTER_LEN);
        fs->read((char *)(&ixAppendPageCounter), IX::FILE_COUNTER_LEN);
        fs->read((char *)(&rootPagePtr), IX::FILE_ROOTPAGE_PTR_LEN);
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
        fs->write((char *)(&rootPagePtr), IX::FILE_ROOTPAGE_PTR_LEN);
        return 0;
    }

    RC IXFileHandle::createRootPage() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        RC ret = 0;
        ret = appendEmptyPage();
        if(ret) {
            return ret;
        }
        rootPagePtr = getLastPageIndex();
        flushMetaData();
        return 0;
    }
    RC IXFileHandle::readRoot() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        if(!isRootPageExist()) {
            return ERR_ROOTPAGE_NOT_EXIST;
        }
        fs->clear();
        fs->seekg(rootPagePtr * PAGE_SIZE, fs->beg);
        fs->read((char *)(&root), IX::FILE_ROOT_LEN);
        ixReadPageCounter++;
        return 0;
    }
    RC IXFileHandle::flushRoot() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekp(rootPagePtr * PAGE_SIZE, fs->beg);
        fs->write((char *)(&root), IX::FILE_ROOT_LEN);
        fs->flush();
        ixWritePageCounter++;
        return 0;
    }

    uint32_t IXFileHandle::getRoot() {
        readRoot();
        return root;
    }

    RC IXFileHandle::setRoot(uint32_t newRoot) {
        root = newRoot;
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        fs->clear();
        fs->seekp(rootPagePtr * PAGE_SIZE, fs->beg);
        fs->write((char *)(&newRoot), IX::FILE_ROOT_LEN);
        fs->flush();
        return 0;
    }

    std::string IXFileHandle::getFileName() {
        return fileName;
    }

    bool IXFileHandle::isOpen() {
        return fs && fs->is_open();
    }

    bool IXFileHandle::isRootPageExist() {
        return rootPagePtr != IX::PAGE_PTR_NULL;
    }

    bool IXFileHandle::isRootNull() {
        return root == IX::PAGE_PTR_NULL;
    }
}

