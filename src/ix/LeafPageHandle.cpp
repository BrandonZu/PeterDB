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

    RC LeafPageHandle::insertEntryWithEnoughSpace(const uint8_t* key, const RID& entry, const Attribute& attr) {
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
                if (isCompositeKeyMeetCompCondition(key, entry, data + pos, curRid, attr, LT_OP)) {
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

        writeEntry(pos, key, entry, attr);

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
        memcpy(data + pos, &entry.pageNum, IX::LEAFPAGE_ENTRY_PAGE_LEN);
        pos += IX::LEAFPAGE_ENTRY_PAGE_LEN;
        memcpy(data + pos, &entry.slotNum, IX::LEAFPAGE_ENTRY_SLOT_LEN);
        pos += IX::LEAFPAGE_ENTRY_SLOT_LEN;
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

    RC LeafPageHandle::getFirstKey(uint8_t* keyData, const Attribute& attr) {
        if(counter < 1) {
            return ERR_KEY_NOT_EXIST;
        }
        int keyLen = getKeyLen(keyData, attr);
        memcpy(keyData, data, keyLen);
        return 0;
    }

    RC LeafPageHandle::splitPageAndInsertEntry(uint8_t* middleKey, uint32_t& newLeafPage, const uint8_t* key, const RID& entry, const Attribute& attr) {
        RC ret = 0;
        // 0. Append a new page
        ret = ixFileHandle.appendEmptyPage();
        if(ret) return ret;
        newLeafPage = ixFileHandle.getLastPageIndex();

        int16_t moveStartIndex = counter / 2;
        // 1. Find move data start position and length
        int16_t moveStartPos = 0;
        for(int16_t i = 0; i < moveStartIndex; i++) {
            moveStartPos += getEntryLen(data + moveStartPos, attr);
        }
        if(moveStartPos >= freeBytePtr) {
            return ERR_PTR_BEYONG_FREEBYTE;
        }

        // 2. Move data to new page and set metadata
        int16_t moveLen = freeBytePtr - moveStartPos;
        LeafPageHandle newPage(ixFileHandle, newLeafPage, nextPtr, data + moveStartPos,
                               moveLen, counter - moveStartIndex);

        // 3. Compact old page and maintain metadata
        freeBytePtr -= moveLen;
        counter = moveStartIndex;

        // 4. Insert new leaf page into the leaf page linked list
        newPage.setNextPtr(this->nextPtr);
        nextPtr = newLeafPage;

        // 5. Insert new entry into old page or new page
        RID tmpRid;
        getRid(data + moveStartPos, attr, tmpRid);
        if(isCompositeKeyMeetCompCondition(key, entry, data + moveStartPos, tmpRid, attr, CompOp::LT_OP)) {
            ret = this->insertEntryWithEnoughSpace(key, entry, attr);
            if(ret) return ret;
        }
        else {
            ret = newPage.insertEntryWithEnoughSpace(key, entry, attr);
            if(ret) return ret;
        }

        // 6. Return new middle key
        newPage.getFirstKey(middleKey, attr);

        return 0;
    }

    RC LeafPageHandle::print(const Attribute &attr, std::ostream &out) {
        RC ret = 0;
        out << "{\"keys\": [";
        int16_t offset = 0;
        uint32_t pageNum;
        int16_t slotNum;
        for(int16_t i = 0; i < counter; i++) {
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
            offset += getKeyLen(data + offset, attr);

            memcpy(&pageNum, data + offset, IX::LEAFPAGE_ENTRY_PAGE_LEN);
            offset += IX::INDEXPAGE_CHILD_PTR_LEN;
            memcpy(&slotNum, data + offset, IX::LEAFPAGE_ENTRY_SLOT_LEN);
            offset += IX::LEAFPAGE_ENTRY_SLOT_LEN;
            out << ":[(" << pageNum << "," << slotNum <<")]\"";

            if(i != counter - 1) {
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
        return getKeyLen(key, attr) + IX::LEAFPAGE_ENTRY_PAGE_LEN + IX::LEAFPAGE_ENTRY_SLOT_LEN;
    }

    void LeafPageHandle::getRid(const uint8_t* keyStartPos, const Attribute& attr, RID& rid) {
        int16_t pos = 0;
        pos += getKeyLen(keyStartPos, attr);
        memcpy(&rid.pageNum, data + getKeyLen(data, attr), IX::LEAFPAGE_ENTRY_PAGE_LEN);
        memcpy(&rid.slotNum, data + getKeyLen(data, attr) + IX::LEAFPAGE_ENTRY_PAGE_LEN, IX::LEAFPAGE_ENTRY_SLOT_LEN);
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
        memcpy(&rid.pageNum, data + pos, IX::LEAFPAGE_ENTRY_PAGE_LEN);
        pos += IX::LEAFPAGE_ENTRY_PAGE_LEN;
        memcpy(&rid.slotNum, data + pos, IX::LEAFPAGE_ENTRY_SLOT_LEN);
        pos += IX::LEAFPAGE_ENTRY_SLOT_LEN;
        return 0;
    }

    int16_t LeafPageHandle::getLeafHeaderLen() {
        return getHeaderLen() + IX::LEAFPAGE_NEXT_PTR_LEN;
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
