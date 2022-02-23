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

        // Create the file
        {
            ofstream out_fs(fileName, ios::binary);
            if (!out_fs.good()) {
                return ERR_CREATE_FILE;
            }
        }

        // Reserve the first page to store metadata
        // Counters * 3
        {
            IXFileHandle fileHandle;
            ret = fileHandle.open(fileName);
            if(ret || !fileHandle.isOpen()) {
                return ERR_CREATE_FILE;
            }
            fileHandle.appendEmptyPage();
        }
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
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attr, const void *key, const RID &rid) {
        RC ret = 0;
        if(!ixFileHandle.isOpen()) {
            return ERR_FILE_NOT_OPEN;
        }
        if(!ixFileHandle.isRootExist()) {
            // Root Page not exist
            ret = ixFileHandle.createRootPage();
            if(ret) {
                return ret;
            }
        }

        if(ixFileHandle.isRootNull()) {
            // Append one leaf page
            ret = ixFileHandle.appendEmptyPage();
            if(ret) {
                return ret;
            }
            uint32_t leafPageNum = ixFileHandle.getLastPageIndex();
            ixFileHandle.root = ixFileHandle.getLastPageIndex();
            LeafPageHandle leafPageHandle(ixFileHandle, leafPageNum, IX::PAGE_PTR_NULL, IX::PAGE_PTR_NULL);
            ret = leafPageHandle.insertEntry((uint8_t*)key, rid, attr);
            if(ret) {
                return ret;
            }
            return 0;
        }

        ret = insertEntryRecur(ixFileHandle, attr, key, rid, ixFileHandle.root);
        if(ret) {
            return ret;
        }
        return 0;
    }

    RC IndexManager::insertEntryRecur(IXFileHandle &ixFileHandle, const Attribute &attr, const void *key,
                                      const RID &rid, uint32_t pageNum) {
        RC ret = 0;
        IXPageHandle pageHandle(ixFileHandle, pageNum);
        if(pageHandle.isTypeIndex()) {
            IndexPageHandle indexPH(pageHandle);
            uint32_t childPage;
            ret = indexPH.getTargetChild(childPage, (uint8_t *)key, attr);
            if(ret) return ret;
            return insertEntryRecur(ixFileHandle, attr, key, rid, childPage);
        }
        else if(pageHandle.isTypeLeaf()) {
            LeafPageHandle oldPH(pageHandle);
            if(oldPH.hasEnoughSpace((uint8_t*)key, attr)) {
                ret = oldPH.insertEntry((uint8_t*)key, rid, attr);
                if(ret) return ret;
                return 0;
            }
            else {
                // Not enough space, need split
                uint32_t newLeafPageNum;
                oldPH.splitPageAndInsertEntry(newLeafPageNum, (uint8_t *) key, rid, attr);
                LeafPageHandle newPH(ixFileHandle, newLeafPageNum);

                if(oldPH.isParentPtrNull()) {
                    // Insert a new index page
                    ret = ixFileHandle.appendEmptyPage();
                    if(ret) return ret;
                    uint32_t newIndexPageNum = ixFileHandle.getLastPageIndex();

                    // Insert new key into index page
                    uint8_t newParentKey[PAGE_SIZE];
                    ret = newPH.getFirstKey(newParentKey, attr);
                    if(ret) return ret;
                    IndexPageHandle newIndexPH(ixFileHandle, newIndexPageNum, IX::PAGE_PTR_NULL,
                                               oldPH.pageNum, newParentKey, newPH.pageNum, attr);

                    // Set pointers
                    oldPH.setParentPtr(newIndexPageNum);
                    newPH.setParentPtr(newIndexPageNum);
                    ixFileHandle.setRoot(newIndexPageNum);
                    return 0;
                }
                else {
                    uint8_t newParentKey[PAGE_SIZE];
                    ret = newPH.getFirstKey(newParentKey, attr);
                    if(ret) return ret;
                    IndexPageHandle newIndexPH(ixFileHandle, oldPH.parentPtr);
                    ret = newIndexPH.insertIndex(newParentKey, attr, newLeafPageNum);
                    if(ret) return ret;
                    return 0;
                }
            }
        }
        else {
            return ERR_PAGE_TYPE_UNKNOWN;
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

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attr, std::ostream &out) const {
        RC ret = 0;
        if(ixFileHandle.isRootNull() || !ixFileHandle.isRootExist()) {
            return ERR_ROOT_NOT_EXIST_OR_NULL;
        }
        IXPageHandle page(ixFileHandle, ixFileHandle.root);
        if(page.isTypeIndex()) {
            IndexPageHandle indexPH(page);
            indexPH.print(attr, out);
        }
        else if(page.isTypeLeaf()) {
            LeafPageHandle leafPH(page);
            leafPH.print(attr, out);
        }
        else {
            return IX::PAGE_TYPE_UNKNOWN;
        }
        return 0;
    }

    bool IndexManager::isFileExists(std::string fileName) {
        struct stat stFileInfo{};
        return stat(fileName.c_str(), &stFileInfo) == 0;
    }

} // namespace PeterDB