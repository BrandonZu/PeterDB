#include "src/include/ix.h"

using std::ios;
using std::fstream;
using std::ifstream;
using std::ofstream;

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        RC ret = 0;
        if(isFileExists(fileName)) {
            return ERR_CREATE_FILE_ALREADY_EXIST;
        }

        // Create the file using ofstream
        ofstream out_fs(fileName, ios::binary);

        // Reserve the first page to store metadata: Counters, Root Page,
        char buffer[PAGE_SIZE];
        bzero(buffer, PAGE_SIZE);
        out_fs.write(buffer, PAGE_SIZE);
        out_fs.flush();
        out_fs.close();
        return 0;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if(!isFileExists(fileName)) {
            return ERR_FILE_NOT_EXIST;
        }

        if(remove(fileName.c_str()) != 0) {
            return ERR_DELETE_FILE;
        }
        return 0;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        RC ret = 0;
        if(!isFileExists(fileName)) {
            return ERR_FILE_NOT_EXIST;
        }

        ret = ixFileHandle.open(fileName);
        if(ret) {
            return ERR_OPEN_FILE;
        }
        return 0;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        if(!isFileExists(ixFileHandle.getFileName())) {
            return ERR_FILE_NOT_EXIST;
        }
        RC rc = ixFileHandle.close();
        return rc;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        RC ret = 0;
        if(!ixFileHandle.isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }

        if(ixFileHandle.isRootNull()) {

            ret = ixFileHandle.appendPage()
        }
        else {

        }
        return 0;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {

        return -1;
    }

    bool IndexManager::isFileExists(std::string fileName) {
        struct stat stFileInfo{};
        return stat(fileName.c_str(), &stFileInfo) == 0;
    }

} // namespace PeterDB