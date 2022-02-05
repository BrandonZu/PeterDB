#include "src/include/pfm.h"

using std::ios;
using std::fstream;
using std::ifstream;
using std::ofstream;

namespace PeterDB {
    bool FileHandle::isOpen() {
        return fs->is_open();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        pageCounter = 0;
        fs = new fstream;
    }

    FileHandle::~FileHandle() {
        flushMetadata();
        delete fs;
    }

    int FileHandle::getCounterNum() {
        return 4;
    }

    void FileHandle::setCounters(const uint32_t counters[]) {
        readPageCounter = counters[0];
        writePageCounter = counters[1];
        appendPageCounter = counters[2];
        pageCounter = counters[3];
    }

    void FileHandle::getCounters(uint32_t counters[]) const {
        counters[0] = readPageCounter;
        counters[1] = writePageCounter;
        counters[2] = appendPageCounter;
        counters[3] = pageCounter;
    }

    RC FileHandle::readMetadata() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }

        fs->seekg(fs->beg);
        int counterNum = PeterDB::FileHandle::getCounterNum();
        uint32_t counters[counterNum];
        for(int i = 0; i < counterNum; i++) {
            fs->read(reinterpret_cast<char *>(counters + i), sizeof(uint32_t));
        }
        setCounters(counters);

        return 0;
    }

    RC FileHandle::flushMetadata() {
        if(!isOpen())
            return ERR_FILE_NOT_OPEN;

        fs->seekp(fs->beg);
        int counterNum = PeterDB::FileHandle::getCounterNum();
        uint32_t counters[counterNum];
        getCounters(counters);
        for(uint32_t i = 0; i < counterNum; i++) {
            fs->write(reinterpret_cast<char *>(counters + i), sizeof(uint32_t));
        }
        fs->flush();
        fs->seekp(fs->beg);
        return 0;
    }

    RC FileHandle::open(const std::string& tmpFileName) {
        // Already bound to a file
        if(isOpen()) {
            return ERR_OPEN_FILE_ALREADY_OPEN;
        }

        fileName = tmpFileName;
        fs->open(tmpFileName, std::fstream::in | std::fstream::in | std::fstream::in);
        if(!isOpen()) {
            return ERR_OPEN_FILE;
        }

        // Read Metadata From the header page
        RC rc = readMetadata();
        if(rc) {
            return rc;
        }
        return 0;
    }

    RC FileHandle::close() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        // Flush all the counters to the hidden file
        flushMetadata();
        fs->close();
        return 0;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // Not Bound to a file
        if(!isOpen())
            return ERR_FILE_NOT_OPEN;
        // Page Not Exist
        if(pageNum >= pageCounter)
            return ERR_PAGE_NOT_EXIST;

        fs->seekg((pageNum + 1) * PAGE_SIZE, fs->beg);  // Page is 1-indexed
        fs->read((char *)data, PAGE_SIZE);
        if(!fs->good()) {
            return ERR_READ_PAGE;
        }
        readPageCounter++;
        // For Robust
        // flushMetadata();
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // Not Bound to a file
        if(!isOpen())
            return ERR_FILE_NOT_OPEN;
        // Page Not Exist
        if(pageNum >= pageCounter)
            return ERR_PAGE_NOT_EXIST;

        fs->seekp((pageNum + 1) * PAGE_SIZE, fs->beg);  // Page is 1-indexed
        fs->write((char *)data, PAGE_SIZE);
        fs->flush(); // IMPORTANT!
        if(!fs->good()) {
            return ERR_WRITE_PAGE;
        }
        writePageCounter++;
        // For Robust
        // flushMetadata();
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        // Not Bound to a file
        if(!isOpen())
            return ERR_FILE_NOT_OPEN;
        fs->clear();
        fs->seekp((pageCounter + 1) * PAGE_SIZE);
        fs->write((char *)data, PAGE_SIZE);
        fs->flush(); // IMPORTANT!
        if(!fs->good()) {
            return ERR_APPEND_PAGE;
        }
        appendPageCounter++;
        pageCounter++;
        // For Robust
        // flushMetadata();
        return 0;
    }

    uint32_t FileHandle::getNumberOfPages() {
        return pageCounter;
    }

    RC FileHandle::collectCounterValues(uint32_t &readPageCount, uint32_t &writePageCount, uint32_t &appendPageCount) {
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }
}
