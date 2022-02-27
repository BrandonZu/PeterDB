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
            LeafPageHandle leafPageHandle(ixFileHandle, leafPage, IX::PAGE_PTR_NULL);
            ret = leafPageHandle.insertEntryWithEnoughSpace((uint8_t *) key, rid, attr);
            if(ret) {
                return ret;
            }
            ixFileHandle.setRoot(leafPage);
            return 0;
        }

        uint8_t newKey[PAGE_SIZE];
        uint32_t newChildPage;
        bool isNewChildExist = false;
        ret = insertEntryRecur(ixFileHandle, ixFileHandle.getRoot(), attr, (uint8_t *)key, rid, newKey, newChildPage, isNewChildExist);
        if(ret) return ret;
        return 0;
    }

    RC IndexManager::insertEntryRecur(IXFileHandle &ixFileHandle, uint32_t pageNum, const Attribute &attr, const uint8_t *keyToInsert, const RID &ridToInsert,
                                      uint8_t* middleKey, uint32_t& newChildPage, bool& isNewChildExist) {
        RC ret = 0;
        int16_t pageType;
        {
            IXPageHandle pageFH(ixFileHandle, pageNum);
            pageType = pageFH.getPageType();
        }

        if(pageType == IX::PAGE_TYPE_INDEX) {
            uint32_t oldChild;
            IndexPageHandle curIndexPH(ixFileHandle, pageNum);
            curIndexPH.getTargetChild(oldChild, keyToInsert, ridToInsert, attr);
            ret = insertEntryRecur(ixFileHandle, oldChild, attr, keyToInsert, ridToInsert, middleKey, newChildPage, isNewChildExist);
            if (ret) return ret;

            if(!isNewChildExist) {
                return 0;
            }
            else {
                ret = curIndexPH.insertIndex(middleKey, newChildPage, isNewChildExist, keyToInsert, ridToInsert, attr, newChildPage);
                if(ret) return ret;
            }
        }
        else if(pageType == IX::PAGE_TYPE_LEAF) {
            LeafPageHandle leafPH(ixFileHandle, pageNum);
            ret = leafPH.insertEntry(keyToInsert, ridToInsert, attr, middleKey, newChildPage, isNewChildExist);
            if(ret) return ret;

            // Corner Case: Leaf node needs to split and there isn't any index page yet
            if(isNewChildExist && pageNum == ixFileHandle.getRoot()) {
                // Insert new index page
                ret = ixFileHandle.appendEmptyPage();
                if(ret) return ret;
                uint32_t newIndexPageNum = ixFileHandle.getLastPageIndex();

                IndexPageHandle newIndexPH(ixFileHandle, newIndexPageNum,
                                           pageNum, middleKey, newChildPage, attr);
                ixFileHandle.setRoot(newIndexPageNum);
            }
        }
        else {
            return ERR_PAGE_TYPE_UNKNOWN;
        }
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
        ret = findTargetLeafNode(ixFileHandle, leafPage, (uint8_t *)key, rid, attribute);
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

    RC IndexManager::findTargetLeafNode(IXFileHandle &ixFileHandle, uint32_t& leafPageNum, const uint8_t* key, const RID& rid, const Attribute& attr) {
        RC ret = 0;
        if(!ixFileHandle.isRootPageExist()) {
            return ERR_ROOTPAGE_NOT_EXIST;
        }
        if(ixFileHandle.isRootNull()) {
            return ERR_ROOT_NULL;
        }

        uint32_t curPageNum = ixFileHandle.getRoot();
        while(curPageNum != IX::PAGE_PTR_NULL && curPageNum < ixFileHandle.getPageCounter()) {
            int16_t pageType;
            {
                IXPageHandle pageFH(ixFileHandle, curPageNum);
                pageType = pageFH.getPageType();
            }

            if (pageType == IX::PAGE_TYPE_LEAF) {
                break;
            }

            IndexPageHandle indexPH(ixFileHandle, curPageNum);
            ret = indexPH.getTargetChild(curPageNum, key, rid, attr);
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

        int16_t pageType;
        {
            IXPageHandle pageFH(ixFileHandle, ixFileHandle.getRoot());
            pageType = pageFH.getPageType();
        }

        if(pageType == IX::PAGE_TYPE_INDEX) {
            IndexPageHandle indexPH(ixFileHandle, ixFileHandle.getRoot());
            indexPH.print(attr, out);
        }
        else if(pageType == IX::PAGE_TYPE_LEAF) {
            LeafPageHandle leafPH(ixFileHandle, ixFileHandle.getRoot());
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