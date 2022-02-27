#include "src/include/ix.h"

namespace PeterDB {
    IndexPageHandle::IndexPageHandle(IXFileHandle& fileHandle, uint32_t page): IXPageHandle(fileHandle, page) {

    }

    // Initialize the new page with one key
    IndexPageHandle::IndexPageHandle(IXFileHandle& fileHandle, uint32_t page,
                                     uint32_t leftPage, uint8_t* key, uint32_t rightPage, const Attribute &attr):
                                     IXPageHandle(fileHandle, page, IX::PAGE_TYPE_INDEX, 0, 1) {
        int16_t pos = 0;
        // Write left page pointer
        memcpy(data + pos, &leftPage, IX::INDEXPAGE_CHILD_PTR_LEN);
        pos += IX::INDEXPAGE_CHILD_PTR_LEN;
        // Write the key
        int16_t keyLen = getKeyLen(key, attr);
        memcpy(data + pos, key, keyLen);
        pos += keyLen;
        // Write right page pointer
        memcpy(data + pos, &rightPage, IX::INDEXPAGE_CHILD_PTR_LEN);
        pos += IX::INDEXPAGE_CHILD_PTR_LEN;

        setFreeBytePointer(pos);
    }

    // Initialize new page with existing entries
    IndexPageHandle::IndexPageHandle(IXFileHandle& fileHandle, uint32_t page,
                                     uint8_t* entryData, int16_t dataLen, int16_t entryCounter):
                                     IXPageHandle(fileHandle, entryData, dataLen, page, IX::PAGE_TYPE_INDEX, dataLen, entryCounter) {
    }

    IndexPageHandle::~IndexPageHandle() = default;

    RC IndexPageHandle::getTargetChild(uint32_t& childPtr, const uint8_t* key, const Attribute &attr) {
        RC ret = 0;
        if(key == nullptr) {    // For scanner, get the first child
            memcpy(&childPtr, data, IX::INDEXPAGE_CHILD_PTR_LEN);
            return 0;
        }
        int16_t pos = 0;
        ret = findPosToInsertKey(pos, key, attr);
        if(ret) return ret;
        // Get previous child pointer
        pos -= IX::INDEXPAGE_CHILD_PTR_LEN;
        memcpy(&childPtr, data + pos, IX::INDEXPAGE_CHILD_PTR_LEN);
        return 0;
    }

    RC IndexPageHandle::findPosToInsertKey(int16_t& keyPos, const uint8_t* key, const Attribute& attr) {
        // 0. Empty Page
        if(counter == 0) {
            keyPos = IX::INDEXPAGE_CHILD_PTR_LEN;
            return 0;
        }

        // 1. Iterate over all following keys
        keyPos = IX::INDEXPAGE_CHILD_PTR_LEN;
        for(int16_t i = 0; i < counter; i++) {
            if(isKeyMeetCompCondition(key, data + keyPos, attr, CompOp::LT_OP)) {
                break;
            }
            keyPos += getEntryLen(data + keyPos, attr);
        }

        return 0;
    }

    RC IndexPageHandle::insertIndex(uint8_t* middleKey, uint32_t& newChildPage, bool& isNewChildExist,
                                    const uint8_t* keyToInsert, const Attribute& attr, uint32_t childPtrToInsert) {
        RC ret = 0;
        if(hasEnoughSpace(keyToInsert, attr)) {
            ret = insertIndexWithEnoughSpace(keyToInsert, attr, childPtrToInsert);
            if(ret) return ret;
            isNewChildExist = false;
        }
        else {
            // Not enough space, need to split index page
            uint8_t middleKey[PAGE_SIZE];
            ret = splitPageAndInsertIndex(middleKey, newChildPage, keyToInsert, attr, childPtrToInsert);
            if(ret) return ret;
            isNewChildExist = true;

            if(ixFileHandle.getRoot() == pageNum) {
                // Insert new index page
                ret = ixFileHandle.appendEmptyPage();
                if(ret) return ret;
                uint32_t newParentPage = ixFileHandle.getLastPageIndex();

                IndexPageHandle newIndexPH(ixFileHandle, newParentPage,
                                           pageNum, middleKey, newChildPage, attr);
                ixFileHandle.setRoot(newParentPage);
            }
        }
        return 0;
    }

    RC IndexPageHandle::insertIndexWithEnoughSpace(const uint8_t* key, const Attribute& attr, uint32_t childPage) {
        RC ret = 0;
        int16_t insertPos = 0;
        ret = findPosToInsertKey(insertPos, key, attr);
        if(ret) return ret;

        if(insertPos > freeBytePtr) {
            return ERR_PTR_BEYONG_FREEBYTE;
        }
        int16_t entryLen = getEntryLen(key, attr);
        if(insertPos < freeBytePtr) {
            // Insert Entry in the middle, Need to shift entries right
            ret = shiftRecordRight(insertPos, entryLen);
            if(ret) {
                return ret;
            }
        }
        writeIndex(insertPos, key, attr, childPage);

        freeBytePtr += entryLen;
        counter++;

        return 0;
    }

    RC IndexPageHandle::writeIndex(int16_t pos, const uint8_t* key, const Attribute& attr, uint32_t newPageNum) {
        RC ret = 0;
        int keyLen = getKeyLen(key, attr);
        memcpy(data + pos, key, keyLen);
        pos += keyLen;
        memcpy(data + pos, &newPageNum, IX::INDEXPAGE_CHILD_PTR_LEN);
        return 0;
    }

    RC IndexPageHandle::splitPageAndInsertIndex(uint8_t * middleKey, uint32_t& newIndexPage, const uint8_t* key, const Attribute& attr, uint32_t child) {
        RC ret = 0;
        // 0. Append a new page
        ret = ixFileHandle.appendEmptyPage();
        if(ret) return ret;
        newIndexPage = ixFileHandle.getLastPageIndex();

        // 1. Push up middle key - 3 Cases
        int16_t newKeyInsertPos;
        ret = findPosToInsertKey(newKeyInsertPos, key, attr);
        if(ret) return ret;

        int16_t curIndex = counter / 2, prevIndex = curIndex - 1;
        int16_t curPos = IX::INDEXPAGE_CHILD_PTR_LEN, prevPos;
        for(int16_t i = 0; i < curIndex; i++) {
            prevPos = curPos;
            curPos += getEntryLen(data + curPos, attr);
        }
        // ... | Prev Key | Cur Key | ...
        int16_t moveStartPos, moveLen;
        if(newKeyInsertPos <= prevPos) {
            // Case 1: Previous Key will be middle key
            // Push middle key
            memcpy(middleKey, data + prevPos, getKeyLen(data + prevPos, attr));
            // Move Data
            moveStartPos = prevPos + getKeyLen(data + prevPos, attr);
            moveLen = freeBytePtr - moveStartPos;
            IndexPageHandle newIndexPH(ixFileHandle, newIndexPage, data + moveStartPos, moveLen, counter - curIndex);
            // Cur Page set counters
            freeBytePtr = prevPos;
            counter = prevIndex;
            // Insert Key into old page
            ret = insertIndexWithEnoughSpace(key, attr, child);
            if(ret) return ret;
        }
        else if(newKeyInsertPos == curPos) {
            // Case 2: New key will be the middle key
            // Push middle key
            memcpy(middleKey, key, getKeyLen(key, attr));
            // Move Data
            uint8_t dataToMove[PAGE_SIZE];
            moveLen = freeBytePtr - curPos + IX::INDEXPAGE_CHILD_PTR_LEN;
            memcpy(dataToMove, &child, IX::INDEXPAGE_CHILD_PTR_LEN);
            memcpy(dataToMove + IX::INDEXPAGE_CHILD_PTR_LEN, key + curPos, freeBytePtr - curPos);
            IndexPageHandle newIndexPH(ixFileHandle, newIndexPage, dataToMove, moveLen, counter - curIndex);
            // Cur Page set counters
            freeBytePtr = curPos;
            counter = prevIndex + 1;
        }
        else {
            // Case 3: Current Key will be middle page
            memcpy(middleKey, data + curPos, getKeyLen(data + curPos, attr));
            // Move Data
            moveStartPos = curPos + getKeyLen(data + curPos, attr);
            moveLen = freeBytePtr - moveStartPos;
            IndexPageHandle newIndexPH(ixFileHandle, newIndexPage, data + moveStartPos, moveLen, counter - curIndex - 1);
            // Cur Page set counters
            freeBytePtr = curPos;
            counter = prevIndex + 1;
            // Insert Key into new page
            ret = newIndexPH.insertIndexWithEnoughSpace(key, attr, child);
            if(ret) return ret;
        }

        return 0;
    }

    int16_t IndexPageHandle::getEntryLen(const uint8_t* key, const Attribute& attr) {
        return getKeyLen(key, attr) + IX::INDEXPAGE_CHILD_PTR_LEN;
    }

    RC IndexPageHandle::print(const Attribute &attr, std::ostream &out) {
        RC ret = 0;
        // 1. Keys
        out << "{\"keys\": [";
        std::queue<int> children;
        int16_t offset = 0;
        uint32_t child;
        for(int16_t i = 0; i < counter; i++) {
            memcpy(&child, data + offset, IX::INDEXPAGE_CHILD_PTR_LEN);
            children.push(child);
            offset += IX::INDEXPAGE_CHILD_PTR_LEN;
            int16_t keyLen = getKeyLen(data + offset, attr);
            out << "\"";
            switch (attr.type) {
                case TypeInt:
                    out << getKeyInt(data + offset);
                    break;
                case TypeReal:
                    out << getKeyReal(data + offset);
                    break;
                case TypeVarChar:
                    out << getKeyString(data + offset);
                    break;
                default:
                    return ERR_KEY_TYPE_NOT_SUPPORT;
            }
            offset += keyLen;
            out << "\"";
            if(i != counter - 1) {
                out << ",";
            }
        }
        // Last Child
        memcpy(&child, data + offset, IX::INDEXPAGE_CHILD_PTR_LEN);
        if(child < IX::FILE_HIDDEN_PAGE_NUM) {
            return ERR_INDEXPAGE_LAST_CHILD_NOT_EXIST;
        }
        children.push(child);
        out << "]," << std::endl;
        // 2. Children
        out << "\"children\": [" << std::endl;
        while(!children.empty()) {
            int16_t pageType;
            {
                IXPageHandle pageHandle(ixFileHandle, children.front());
                pageType = pageHandle.getPageType();
            }
            if(pageType == IX::PAGE_TYPE_INDEX) {
                IndexPageHandle indexPH(ixFileHandle, children.front());
                indexPH.print(attr, out);
            }
            else if(pageType == IX::PAGE_TYPE_LEAF) {
                LeafPageHandle leafPH(ixFileHandle, children.front());
                leafPH.print(attr, out);
            }
            children.pop();
            if(!children.empty()) {
                out << ",";
            }
            out << std::endl;
        }
        out << "]}";
        return 0;
    }


    bool IndexPageHandle::hasEnoughSpace(const uint8_t* key, const Attribute &attr) {
        return getFreeSpace() >= getKeyLen(key, attr) + IX::INDEXPAGE_CHILD_PTR_LEN;
    }

    int16_t IndexPageHandle::getIndexHeaderLen() {
        return getHeaderLen();
    }

    int16_t IndexPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePtr - getIndexHeaderLen();
    }

}
