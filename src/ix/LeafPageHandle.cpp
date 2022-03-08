#include "src/include/ix.h"

namespace PeterDB {
    LeafPageHandle::LeafPageHandle(IXFileHandle& fileHandle, uint32_t page): IXPageHandle(fileHandle, page) {
        nextPtr = getNextPtrFromData();
    }

    LeafPageHandle::LeafPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t next):
        IXPageHandle(fileHandle, page, IX::PAGE_TYPE_LEAF, 0, 0) {
        setNextPtr(next);
    }

    LeafPageHandle::LeafPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t next,
                                   uint8_t* entryData, int16_t dataLen, int16_t entryCounter):
                                   IXPageHandle(fileHandle, entryData, dataLen, page, IX::PAGE_TYPE_LEAF, dataLen, entryCounter) {
        setNextPtr(next);
    }

    LeafPageHandle::~LeafPageHandle() {
        setNextPtr(nextPtr);
    }

    RC LeafPageHandle::insertEntry(const uint8_t* key, const RID& rid, const Attribute& attr, uint8_t* middleKey, uint32_t& newChild, bool& isNewChildExist) {
        RC ret = 0;
        if(hasEnoughSpace(key, attr)) {
            ret = insertEntryWithEnoughSpace(key, rid, attr);
            if(ret) return ret;
            isNewChildExist = false;
        }
        else {
            // Not enough space in Leaf Page, split Leaf Page
            ret = splitPageAndInsertEntry(middleKey, newChild, key, rid, attr);
            if(ret) return ret;
            isNewChildExist = true;
        }
        return 0;
    }

    RC LeafPageHandle::insertEntryWithEnoughSpace(const uint8_t* key, const RID& rid, const Attribute& attr) {
        RC ret = 0;
        if(getFreeSpace() < getEntryLen(key, attr)) {
            return ERR_PAGE_NOT_ENOUGH_SPACE;
        }

        int16_t pos = 0;
        if(counter == 0) {
            // Case 1: the first entry
            pos = 0;
        }
        else {
            // Case 2: Iterate over other keys
            RID curRid;
            pos = 0;
            for (int16_t index = 0; index < counter; index++) {
                getRid(data + pos, attr, curRid);
                if (isCompositeKeyMeetCompCondition(key, rid, data + pos, curRid, attr, LT_OP)) {
                    break;
                }
                pos += getEntryLen(data + pos, attr);
            }
        }

        if(pos > freeBytePtr) {
            return ERR_PTR_BEYONG_FREEBYTE;
        }

        int16_t entryLen = getEntryLen(key, attr);
        if(pos < freeBytePtr) {
            // Insert Entry in the middle, Need to shift entries right
            ret = shiftRecordRight(pos, entryLen);
            if(ret) {
                return ret;
            }
        }

        writeEntry(pos, key, rid, attr);

        freeBytePtr += entryLen;
        counter++;

        return 0;
    }

    RC LeafPageHandle::writeEntry(int16_t pos, const uint8_t* key, const RID& entry, const Attribute& attr) {
        // Write Key
        int keyLen = getKeyLen(key, attr);
        memcpy(data + pos, key, keyLen);
        pos += keyLen;
        // Write RID
        memcpy(data + pos, &entry.pageNum, IX::PAGE_RID_PAGE_LEN);
        pos += IX::PAGE_RID_PAGE_LEN;
        memcpy(data + pos, &entry.slotNum, IX::PAGE_RID_SLOT_LEN);
        pos += IX::PAGE_RID_SLOT_LEN;
        return 0;
    }

    RC LeafPageHandle::findFirstKeyMeetCompCondition(int16_t& pos, const uint8_t* key, const Attribute& attr, CompOp op) {
        pos = 0;
        for (int16_t index = 0; index < counter; index++) {
            if (isKeyMeetCompCondition(data + pos, key, attr, op)) {
                break;
            }
            pos += getEntryLen(data + pos, attr);
        }
        return 0;
    }

    RC LeafPageHandle::findFirstCompositeKeyMeetCompCondition(int16_t& pos, const uint8_t* key, const RID& rid, const Attribute& attr, CompOp op) {
        pos = 0;
        RID curRID;
        for (int16_t index = 0; index < counter; index++) {
            getRid(data + pos, attr, curRID);
            if (isCompositeKeyMeetCompCondition(key, rid, data + pos, curRID, attr, op)) {
                break;
            }
            pos += getEntryLen(data + pos, attr);
        }
        return 0;
    }

    RC LeafPageHandle::deleteEntry(const uint8_t* key, const RID& entry, const Attribute& attr) {
        RC ret = 0;
        int16_t slotPos = 0;
        findFirstCompositeKeyMeetCompCondition(slotPos, (uint8_t *)key, entry, attr, EQ_OP);
        if(slotPos >= freeBytePtr) {
            return ERR_LEAFNODE_ENTRY_NOT_EXIST;
        }
        int16_t curEntryLen = getEntryLen(data + slotPos, attr);
        int16_t dataNeedMovePos = slotPos + curEntryLen;
        if(dataNeedMovePos < freeBytePtr) {
            shiftRecordLeft(dataNeedMovePos, curEntryLen);
        }
        freeBytePtr -= curEntryLen;
        counter--;
        return 0;
    }

    RC LeafPageHandle::getFirstCompKey(uint8_t* compKeyData, const Attribute& attr) {
        if(counter < 1) {
            return ERR_KEY_NOT_EXIST;
        }
        int compKeyLen = getEntryLen(data, attr);
        memcpy(compKeyData, data, compKeyLen);
        return 0;
    }

    RC LeafPageHandle::splitPageAndInsertEntry(uint8_t* middleKey, uint32_t& newLeafPage, const uint8_t* key, const RID& rid, const Attribute& attr) {
        RC ret = 0;
        // 0. Append a new page
        ret = ixFileHandle.appendEmptyPage();
        if(ret) return ret;
        newLeafPage = ixFileHandle.getLastPageIndex();

        // 1. Find move data start position and index
        // IMPORTANT make sure new entry can be inserted into current page or new page
        int16_t moveStartPos = 0;
        int16_t moveStartIndex = 0;
        while(moveStartPos < freeBytePtr / 2) {
            moveStartPos += getEntryLen(data + moveStartPos, attr);
            moveStartIndex++;
        }
        // Check if this plan is feasible
        bool isSplitFeasible = true;
        RID tmpRid;
        getRid(data + moveStartPos, attr, tmpRid);
        if(isCompositeKeyMeetCompCondition(key, rid, data + moveStartPos, tmpRid, attr, CompOp::LT_OP)) {
            if(moveStartPos + getEntryLen(key, attr) > getMaxFreeSpace()) {
                isSplitFeasible = false;
                moveStartIndex--;
            }
        }
        else {
            if(freeBytePtr - moveStartPos + getEntryLen(key, attr) > getMaxFreeSpace()) {
                isSplitFeasible = false;
                moveStartIndex++;
            }
        }

        if(!isSplitFeasible) {
            moveStartPos = 0;
            for(int16_t i = 0; i < moveStartIndex; i++) {
                moveStartPos += getEntryLen(data + moveStartPos, attr);
            }
        }

        // 2. Move data to new page and set metadata
        int16_t moveLen = freeBytePtr - moveStartPos;
        LeafPageHandle newLeafPageHandle(ixFileHandle, newLeafPage, nextPtr, data + moveStartPos,
                                         moveLen, counter - moveStartIndex);

        // 3. Compact old page and maintain metadata
        freeBytePtr -= moveLen;
        counter = moveStartIndex;

        // 4. Insert new leaf page into the leaf page linked list
        newLeafPageHandle.setNextPtr(this->nextPtr);
        nextPtr = newLeafPage;

        // 5. Insert new entry into old page or new page
        getRid(data + moveStartPos, attr, tmpRid);
        if(isCompositeKeyMeetCompCondition(key, rid, data + moveStartPos, tmpRid, attr, CompOp::LT_OP)) {
            ret = this->insertEntryWithEnoughSpace(key, rid, attr);
            if(ret) return ret;
        }
        else {
            ret = newLeafPageHandle.insertEntryWithEnoughSpace(key, rid, attr);
            if(ret) return ret;
        }

        // 6. Return new middle composite key
        newLeafPageHandle.getFirstCompKey(middleKey, attr);

        return 0;
    }

    RC LeafPageHandle::print(const Attribute &attr, std::ostream &out) {
        RC ret = 0;
        out << "{\"keys\": [";
        int16_t offset = 0;
        uint32_t pageNum;
        int16_t slotNum;

        int32_t curInt;
        float curFloat;
        std::string curStr;

        int16_t i = 0;
        while(i < counter) {
            out << "\"";
            // Print Key
            switch (attr.type) {
                case TypeInt:
                    curInt = getKeyInt(data + offset);
                    out << curInt << ":[";
                    break;
                case TypeReal:
                    curFloat = getKeyReal(data + offset);
                    out << curFloat << ":[";
                    break;
                case TypeVarChar:
                    curStr = getKeyString(data + offset);
                    out << curStr << ":[";
                    break;
                default:
                    return ERR_KEY_TYPE_NOT_SUPPORT;
            }
            // Print following Rid with the same key
            while(i < counter) {
                offset += getKeyLen(data + offset, attr);

                memcpy(&pageNum, data + offset, IX::PAGE_RID_PAGE_LEN);
                offset += IX::PAGE_RID_PAGE_LEN;
                memcpy(&slotNum, data + offset, IX::PAGE_RID_SLOT_LEN);
                offset += IX::PAGE_RID_SLOT_LEN;
                out << "(" << pageNum << "," << slotNum <<")";
                i++;

                if(i >= counter) {
                    break;
                }

                bool isKeySame = true;
                switch (attr.type) {
                    case TypeInt:
                        if(getKeyInt(data + offset) != curInt)
                            isKeySame = false;
                        break;
                    case TypeReal:
                        if(getKeyReal(data + offset) != curFloat)
                            isKeySame = false;
                        break;
                    case TypeVarChar:
                        if(getKeyString(data + offset) != curStr)
                            isKeySame = false;
                        break;
                    default:
                        return ERR_KEY_TYPE_NOT_SUPPORT;
                }
                if(!isKeySame) {
                    break;
                }

                out << ",";
            }
            out << "]\"";

            if(i < counter) {
                out << ",";
            }
        }
        out << "]}";
        return 0;
    }

    bool LeafPageHandle::hasEnoughSpace(const uint8_t* key, const Attribute &attr) {
        return getFreeSpace() >= getEntryLen(key, attr);
    }
    int16_t LeafPageHandle::getEntryLen(const uint8_t* key, const Attribute& attr) {
        return getKeyLen(key, attr) + IX::PAGE_RID_PAGE_LEN + IX::PAGE_RID_SLOT_LEN;
    }

    int16_t LeafPageHandle::getNextEntryPos(int16_t curEntryPos, const Attribute &attr) {
        return curEntryPos + getEntryLen(data + curEntryPos, attr);
    }
    RC LeafPageHandle::getEntry(int16_t pos, uint8_t* key, RID &rid, const Attribute &attr) {
        if(pos >= freeBytePtr) {
            return ERR_PTR_BEYONG_FREEBYTE;
        }
        int16_t keyLen = getKeyLen(data + pos, attr);
        memcpy(key, data + pos, keyLen);
        pos += keyLen;
        memcpy(&rid.pageNum, data + pos, IX::PAGE_RID_PAGE_LEN);
        pos += IX::PAGE_RID_PAGE_LEN;
        memcpy(&rid.slotNum, data + pos, IX::PAGE_RID_SLOT_LEN);
        pos += IX::PAGE_RID_SLOT_LEN;
        return 0;
    }

    int16_t LeafPageHandle::getLeafHeaderLen() {
        return getHeaderLen() + IX::LEAFPAGE_NEXT_PTR_LEN;
    }

    int16_t LeafPageHandle::getMaxFreeSpace() {
        return PAGE_SIZE - getLeafHeaderLen();
    }
    int16_t LeafPageHandle::getFreeSpace() {
        return PAGE_SIZE - getLeafHeaderLen() - freeBytePtr;
    }
    bool LeafPageHandle::isEmpty() {
        return counter == 0;
    }

    int16_t LeafPageHandle::getNextPtrOffset() {
        return PAGE_SIZE - getHeaderLen() - IX::LEAFPAGE_NEXT_PTR_LEN;
    }
    uint32_t LeafPageHandle::getNextPtr() {
        return nextPtr;
    }
    uint32_t LeafPageHandle::getNextPtrFromData() {
        uint32_t nextPtr;
        memcpy(&nextPtr, data + getNextPtrOffset(), IX::LEAFPAGE_NEXT_PTR_LEN);
        return nextPtr;
    }
    void LeafPageHandle::setNextPtr(uint32_t next) {
        memcpy(data + getNextPtrOffset(), &next, IX::LEAFPAGE_NEXT_PTR_LEN);
        this->nextPtr = next;
    }
}
