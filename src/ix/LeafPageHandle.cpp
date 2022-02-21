#include "src/include/ix.h"

namespace PeterDB {
    LeafPageHandle::LeafPageHandle(IXPageHandle& pageHandle): IXPageHandle(pageHandle) {
        nextPtr = getNextPtr();
    }

    LeafPageHandle::LeafPageHandle(IXFileHandle& fileHandle, uint32_t page, uint32_t parent, uint32_t next): IXPageHandle(fileHandle, page) {
        setPageType(IX::PAGE_TYPE_LEAF);
        setCounter(0);
        setFreeBytePointer(0);
        setParentPtr(parent);
        setNextPtr(next);
    }

    LeafPageHandle::~LeafPageHandle() {
        fh.writePage(pageNum, data);
    }

    RC LeafPageHandle::insertEntry(const uint8_t* key, const RID& entry, const Attribute& attr) {
        RC ret = 0;
        if(getFreeSpace() < getEntryLen(key, attr)) {
            return ERR_PAGE_NOT_ENOUGH_SPACE;
        }

        // Iterate over all entries
        int16_t pos = 0;
        for(int16_t index = 0; index < counter; index++) {
            int16_t keyLen = getKeyLen(data + pos, attr);
            if(!isKeyGreater(key, data + pos, attr)) {
                break;
            }
            pos += keyLen + IX::LEAFPAGE_ENTRY_PAGE_LEN + IX::LEAFPAGE_ENTRY_SLOT_LEN;
        }

        if(pos > freeBytePtr) {
            return ERR_PAGE_INTERNAL;
        }

        int16_t keyLen = getKeyLen(key, attr);
        if(pos < freeBytePtr) {
            // Insert Entry in the middle, Need to shift entries right
            ret = shiftRecordRight(pos, keyLen);
            if(ret) {
                return ret;
            }
        }
        writeEntry(pos, key, entry, attr);
        // Update Free Byte Ptr
        setFreeBytePointer(freeBytePtr + keyLen);
        // Update Counter
        addCounterByOne();

        // Flush Page
        ret = fh.writePage(pageNum, data);
        if(ret) {
            return ret;
        }
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
    }

    bool LeafPageHandle::hasEnoughSpace(const uint8_t* key, const Attribute &attr) {
        return getFreeSpace() >= getEntryLen(key, attr);
    }
    int16_t LeafPageHandle::getEntryLen(const uint8_t* key, const Attribute& attr) {
        return getKeyLen(key, attr) + IX::LEAFPAGE_ENTRY_PAGE_LEN + IX::LEAFPAGE_ENTRY_SLOT_LEN;
    }
    int16_t LeafPageHandle::getKeyLen(const uint8_t* key, const Attribute &attr) {
        switch (attr.type) {
            case TypeInt:
                return sizeof(int32_t);
                break;
            case TypeReal:
                return sizeof(float);
                break;
            case TypeVarChar:
                int32_t strLen;
                memcpy(&strLen, key, sizeof(int32_t));
                return strLen + sizeof(int32_t);
                break;
            default:
                return -1;
        }
    }
    int16_t LeafPageHandle::getLeafHeaderLen() {
        return getHeaderLen() + IX::LEAFPAGE_NEXT_PTR_LEN;
    }
    int16_t LeafPageHandle::getFreeSpace() {
        return PAGE_SIZE - getLeafHeaderLen() - freeBytePtr;
    }

    int16_t LeafPageHandle::getNextPtrOffset() {
        return PAGE_SIZE - getHeaderLen() - IX::LEAFPAGE_NEXT_PTR_LEN;
    }
    uint32_t LeafPageHandle::getNextPtr() {
        uint32_t nextPtr;
        memcpy(&nextPtr, data + getNextPtrOffset(), IX::LEAFPAGE_NEXT_PTR_LEN);
        return nextPtr;
    }
    void LeafPageHandle::setNextPtr(uint32_t next) {
        memcpy(data + getNextPtrOffset(), &nextPtr, IX::LEAFPAGE_NEXT_PTR_LEN);
    }
}
