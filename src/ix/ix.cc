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
        if(!ixFileHandle.isRootPageExist()) {
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
            uint32_t leafPage = ixFileHandle.getLastPageIndex();
            LeafPageHandle leafPageHandle(ixFileHandle, leafPage, IX::PAGE_PTR_NULL, IX::PAGE_PTR_NULL);
            ret = leafPageHandle.insertEntryWithEnoughSpace((uint8_t *) key, rid, attr);
            if(ret) {
                return ret;
            }
            ixFileHandle.setRoot(leafPage);
            return 0;
        }

        uint32_t leafPageNum;
        ret = findTargetLeafNode(ixFileHandle, leafPageNum, (uint8_t*)key, attr);
        if(ret) return ret;

        LeafPageHandle leafPH(ixFileHandle, leafPageNum);
        ret = leafPH.insertEntry((uint8_t *)key, rid, attr);
        if(ret) return ret;
        return 0;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        RC ret = 0;
        if(!ixFileHandle.isRootPageExist()) {
            return ERR_ROOTPAGE_NOT_EXIST;
        }
        if(ixFileHandle.isRootNull()) {
            return ERR_ROOT_NULL;
        }

        uint32_t leafPage;
        ret = findTargetLeafNode(ixFileHandle, leafPage, (uint8_t *)key, attribute);
        if(ret) return ret;
        LeafPageHandle leafPH(ixFileHandle, leafPage);
        ret = leafPH.deleteEntry((uint8_t *)key, rid, attribute);
        if(ret) return ret;
        return 0;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        RC ret = 0;
        ret = ix_ScanIterator.open(&ixFileHandle, attribute, (uint8_t*)lowKey, (uint8_t*)highKey, lowKeyInclusive, highKeyInclusive);
        if(ret) return ret;
        return 0;
    }

    RC IndexManager::findTargetLeafNode(IXFileHandle &ixFileHandle, uint32_t& leafPageNum, const uint8_t* key, const Attribute& attr) {
        RC ret = 0;
        if(!ixFileHandle.isRootPageExist()) {
            return ERR_ROOTPAGE_NOT_EXIST;
        }
        if(ixFileHandle.isRootNull()) {
            return ERR_ROOT_NULL;
        }

        uint32_t curPageNum = ixFileHandle.getRoot();
        while(curPageNum != IX::PAGE_PTR_NULL && curPageNum < ixFileHandle.getPageCounter()) {
            IXPageHandle curPagePH(ixFileHandle, curPageNum);
            if(curPagePH.isTypeLeaf()) {
                break;
            }
            IndexPageHandle indexPH(curPagePH);
            ret = indexPH.getTargetChild(curPageNum, key, attr);
            if (ret) return ret;
        }
        if(curPageNum != IX::PAGE_PTR_NULL && curPageNum < ixFileHandle.getPageCounter()) {
            leafPageNum = curPageNum;
        }
        else {
            return ERR_LEAF_NOT_FOUND;
        }
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attr, std::ostream &out) const {
        RC ret = 0;
        if(!ixFileHandle.isRootPageExist()) {
            return ERR_ROOTPAGE_NOT_EXIST;
        }
        if(ixFileHandle.isRootNull()) {
            return ERR_ROOT_NULL;
        }

        IXPageHandle page(ixFileHandle, ixFileHandle.getRoot());
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