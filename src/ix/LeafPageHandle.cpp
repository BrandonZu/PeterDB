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

        int16_t pos = 0;

        if(counter == 0) {
            // Case 1: the first entry
            pos = 0;
        }
        else if(isKeySatisfyComparison(key, data, attr, CompOp::LT_OP)) {
            // Case 2: new key is the smallest
            pos = 0;
        }
        else {
            // Case 3: Iterate over other keys
            int16_t prev = 0;
            pos = getEntryLen(key, attr);
            for (int16_t index = 1; index < counter; index++) {
                if (isKeySatisfyComparison(key, data + prev, attr, CompOp::GE_OP) &&
                    isKeySatisfyComparison(key, data + pos, attr, CompOp::LT_OP)) {
                    break;
                }
                prev = pos;
                pos += getEntryLen(data + pos, attr);
            }
        }

        if(pos > freeBytePtr) {
            return ERR_PAGE_INTERNAL;
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
        // Update Free Byte Ptr
        setFreeBytePointer(freeBytePtr + entryLen);
        // Update Counter
        addCounterByOne();

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
