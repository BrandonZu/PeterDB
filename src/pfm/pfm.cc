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
        if(isFileExists(fileName)) {
            return ERR_CREATE_FILE_ALREADY_EXIST;
        }

        // Create the file using ofstream
        ofstream out_fs(fileName, ios::binary);

        // Reserve the first page to store metadata, e.g. counters, number of pages
        char buffer[PAGE_SIZE];
        bzero(buffer, PAGE_SIZE);
        out_fs.write(buffer, PAGE_SIZE);
        out_fs.flush();
        out_fs.close();
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if(!isFileExists(fileName)) {
            return ERR_FILE_NOT_EXIST;
        }

        if(remove(fileName.c_str()) != 0) {
            return ERR_DELETE_FILE;
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        RC ret = 0;
        if(!isFileExists(fileName)) {
            return ERR_FILE_NOT_EXIST;
        }

        ret = fileHandle.open(fileName);
        if(ret) {
            return ERR_OPEN_FILE;
        }
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if(!isFileExists(fileHandle.fileName)) {
            return ERR_FILE_NOT_EXIST;
        }
        RC rc = fileHandle.close();
        return rc;
    }

    bool PagedFileManager::isFileExists(std::string fileName) {
        struct stat stFileInfo{};
        return stat(fileName.c_str(), &stFileInfo) == 0;
    }

} // namespace PeterDB