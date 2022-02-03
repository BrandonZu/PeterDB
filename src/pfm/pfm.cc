#include "src/include/pfm.h"

using std::ios;
using std::fstream;
using std::ifstream;
using std::ofstream;

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        // Check if this file already exists
        ifstream in_fs(fileName, ios::binary);
        if(in_fs.is_open()) {
            in_fs.close();
            return ERR_CREATE_FILE_ALREADY_EXIST;
        }
        // Create the file using ofstream
        ofstream out_fs(fileName, ios::binary);
        // Reserve the first page to store metadata, e.g. counters, number of pages
        char* buffer = new char[PAGE_SIZE];
        for(int i = 0; i < PAGE_SIZE; i++) {
            buffer[i] = 0;
        }
        out_fs.write(buffer, PAGE_SIZE);
        out_fs.close();
        delete[] buffer;
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if(remove(fileName.c_str()) != 0) {
            return ERR_DELETE_FILE;
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        RC ret = 0;
        // Check if this file exists
        fstream* tmp_fs = new fstream;
        tmp_fs->open(fileName, ios::in | ios::out | ios::binary);

        if(!tmp_fs->is_open()) {
            return ERR_OPEN_FILE_ALREADY_OPEN;
        }

        ret = fileHandle.open(fileName, tmp_fs);
        if(ret) {
            return ERR_OPEN_FILE;
        }
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        RC rc = fileHandle.close();
        return rc;
    }

    bool FileHandle::isOpen() {
        return fs && fs->is_open();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        pageCounter = 0;
        fs = nullptr;
    }

    FileHandle::~FileHandle() = default;

    int FileHandle::getCounterNum() {
        return 4;
    }

    void FileHandle::setCounters(const unsigned counters[]) {
        readPageCounter = counters[0];
        writePageCounter = counters[1];
        appendPageCounter = counters[2];
        pageCounter = counters[3];
    }

    void FileHandle::getCounters(unsigned counters[]) const {
        counters[0] = readPageCounter;
        counters[1] = writePageCounter;
        counters[2] = appendPageCounter;
        counters[3] = pageCounter;
    }

    RC FileHandle::readMetadata() {
        if(!isOpen())
            return ERR_FILE_NOT_OPEN;

        fs->seekg(fs->beg);
        int counterNum = PeterDB::FileHandle::getCounterNum();
        unsigned counters[counterNum];
        for(int i = 0; i < counterNum; i++) {
            fs->read(reinterpret_cast<char *>(counters + i), sizeof(unsigned));
        }
        setCounters(counters);

        return 0;
    }

    RC FileHandle::flushMetadata() {
        if(!isOpen())
            return ERR_FILE_NOT_OPEN;

        fs->seekp(fs->beg);
        int counterNum = PeterDB::FileHandle::getCounterNum();
        unsigned counters[counterNum];
        getCounters(counters);
        for(int i = 0; i < counterNum; i++) {
            fs->write(reinterpret_cast<char *>(counters + i), sizeof(unsigned));
        }
        fs->flush();
        fs->seekp(fs->beg);
        return 0;
    }

    RC FileHandle::open(const std::string& tmpFileName, std::fstream* tmpFS) {
        // Already bound to a file
        if(isOpen()) {
            return ERR_OPEN_FILE_ALREADY_OPEN;
        }

        fs = tmpFS;
        fileName = tmpFileName;
        // Read Metadata From the header page
        RC rc = readMetadata();
        if(rc) return rc;
        return 0;
    }

    RC FileHandle::close() {
        if(!isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }

        // Flush all the counters to the hidden file
        flushMetadata();
        fs->close();
        delete fs;
        return 0;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // Not Bound to a file
        if(!isOpen())
            return ERR_FILE_NOT_OPEN;
        // Page Not Exist
        if(pageNum >= pageCounter)
            return ERR_PAGE_NOT_EXIST;

        fs->seekg((pageNum + 1) * PAGE_SIZE, fs->beg);
        fs->read((char *)data, PAGE_SIZE);
        if(!fs->good())
            return -1;
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

        fs->seekp((pageNum + 1) * PAGE_SIZE, fs->beg);
        fs->write((char *)data, PAGE_SIZE);
        fs->flush(); // IMPORTANT!
        if(!fs->good())
            return -1;
        writePageCounter++;
        // For Robust
        // flushMetadata();
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        // Not Bound to a file
        if(!isOpen())
            return ERR_FILE_NOT_OPEN;
        fs->seekp((pageCounter + 1) * PAGE_SIZE);
        fs->write((char *)data, PAGE_SIZE);
        fs->flush(); // IMPORTANT!
        if(!fs->good())
            return -1;
        appendPageCounter++;
        pageCounter++;
        // For Robust
        // flushMetadata();
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return pageCounter;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }

} // namespace PeterDB