#include "src/include/rbfm.h"

namespace PeterDB {
    RecordPageHandle::RecordPageHandle(FileHandle& fileHandle, PageNum pageNum): fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, data);
        freeBytePointer = getFreeBytePointer();
        slotCounter = getSlotCounter();
    }

    RecordPageHandle::~RecordPageHandle() {
        fh.writePage(pageNum, data);
    }

    /*
     * Read Record
     */

    RC RecordPageHandle::getRecordByteSeq(int16_t slotNum, uint8_t *recordByteSeq, int16_t& recordLen) {
        if(slotNum > slotCounter || isRecordDeleted(slotNum)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << slotNum << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::getRecordByteSeq" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }

        int16_t recordOffset = getRecordOffset(slotNum);
        recordLen = getRecordLen(slotNum);

        // Read Record Byte Seq
        memcpy(recordByteSeq, data + recordOffset, recordLen);

        return 0;
    }

    RC RecordPageHandle::getRecordPointerTarget(const int16_t curSlotNum, int& ptrPageNum, int16_t& ptrSlotNum) {
        if(curSlotNum > slotCounter || isRecordDeleted(curSlotNum)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << curSlotNum << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::getRecordPointerTarget" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }

        int16_t recordOffset = getRecordOffset(curSlotNum);
        int8_t mask;
        memcpy(&mask, data + recordOffset, RECORD_MASK_LEN);
        if(mask != RECORD_MASK_PTRRECORD) {
            LOG(ERROR) << "Record is not a pointer!" << std::endl;
            return -1;
        }
        memcpy(&ptrPageNum, data + recordOffset + RECORD_MASK_LEN, PTRRECORD_PAGE_INDEX_LEN);
        memcpy(&ptrSlotNum, data + recordOffset + RECORD_MASK_LEN + PTRRECORD_PAGE_INDEX_LEN, PTRRECORD_SLOT_INDEX_LEN);
        return 0;
    }

    RC RecordPageHandle::getRecordAttr(int16_t slotNum, int16_t attrIndex, uint8_t* attrData) {
        if(slotNum > slotCounter || isRecordDeleted(slotNum)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << slotNum << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::getRecordAttr" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }

        int16_t recordOffset = getRecordOffset(slotNum);
        int16_t attrOffset = getAttrBeginPos(slotNum, attrIndex);
        int16_t attrLen = getAttrLen(slotNum, attrIndex);
        memcpy(attrData, data + recordOffset + attrOffset, attrLen);

        return 0;
    }

    RC RecordPageHandle::getNextRecord(uint16_t& slotIndex, uint8_t* byteSeq, int16_t& recordLen) {
        RC ret = 0;
        if(slotIndex > slotCounter) {
            return ERR_NEXT_RECORD_NOT_EXIST;
        }
        uint16_t realRecordIndex;
        for(realRecordIndex = slotIndex + 1; realRecordIndex <= slotCounter; realRecordIndex++) {
            if(!isRecordDeleted(realRecordIndex) && !isRecordPointer(realRecordIndex)) {
                break;
            }
        }
        if(realRecordIndex > slotCounter) {
            return ERR_NEXT_RECORD_NOT_EXIST;
        }
        // Return the next real record to the caller
        slotIndex = realRecordIndex;
        ret = getRecordByteSeq(slotIndex, byteSeq, recordLen);
        if(ret) {
            return ret;
        }
        return 0;
    }

    /*
     * Insert Record
     */
    RC RecordPageHandle::insertRecord(uint8_t byteSeq[], int16_t byteSeqLen, RID& rid) {
        RC ret = 0;

        // Get Slot Index while maintaining SlotCounter
        int16_t slotIndex = findAvailSlot();

        // Pad the data if necessary and append padded record data to the end
        uint16_t recordLen = std::max(byteSeqLen, RECORD_MIN_LEN);
        memcpy(data + freeBytePointer, byteSeq, byteSeqLen);

        // Fill in offset and length of record
        setRecordOffset(slotIndex, freeBytePointer);
        setRecordLen(slotIndex, recordLen);

        // Update Free Space Pointer
        freeBytePointer += recordLen;
        setFreeBytePointer(freeBytePointer);

        // Flush page
        ret = fh.writePage(pageNum, data);
        if(ret) {
            LOG(ERROR) << "Fail to flush page data into file! @ RecordPageHandle::insertRecord" << std::endl;
            return ret;
        }

        rid.pageNum = this->pageNum;
        rid.slotNum = slotIndex;

        return 0;
    }

    /*
     * Delete Record
     */
    RC RecordPageHandle::deleteRecord(const int16_t slotIndex) {
        RC ret = 0;
        if(slotIndex > slotCounter) {
            LOG(ERROR) << "Slot not exist! SlotIndex: " << slotIndex << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::deleteRecord" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }
        if(isRecordDeleted(slotIndex)) {
            LOG(ERROR) << "Record is already deleted @ RecordPageHandle::deleteRecord" << std::endl;
            return 0;
        }

        // Get Record Offset and Length
        int16_t recordOffset = getRecordOffset(slotIndex);
        int16_t recordLen = getRecordLen(slotIndex);

        // Shift records left to fill the emtpy hole
        ret = shiftRecord(recordOffset + recordLen, recordLen, true);
        if(ret) {
            LOG(ERROR) << "Fail to shift records left @ RecordPageHandle::deleteRecord" << std::endl;
            return ret;
        }

        // Update current slot to make it empty
        setRecordOffset(slotIndex, PAGE_EMPTY_SLOT_OFFSET);
        setRecordLen(slotIndex, PAGE_EMPTY_SLOT_LEN);

        // Flush page to disk, actually OS handle it, may not flush to disk immediately
        ret = fh.writePage(pageNum, data);
        if(ret) {
            LOG(ERROR) << "Fail to flush page data into file @ RecordPageHandle::deleteRecord" << std::endl;
        }
        return ret;
    }

    bool RecordPageHandle::isRecordDeleted(int16_t slotIndex) {
        int16_t recordOffset = getRecordOffset(slotIndex);
        return recordOffset < 0;
    }

    /*
     * Update Record
     */
    RC RecordPageHandle::updateRecord(int16_t slotIndex, uint8_t byteSeq[], int16_t recordLen) {
        RC ret = 0;
        if(slotIndex > slotCounter || isRecordDeleted(slotIndex)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << slotIndex << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::deleteRecord" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }

        int16_t recordOffset = getRecordOffset(slotIndex);
        int16_t oldRecordLen = getRecordLen(slotIndex);

        // Shift record left or right based on current and former record length
        if(recordLen <= oldRecordLen) {
            shiftRecord(recordOffset + oldRecordLen, oldRecordLen - recordLen, true);
        }
        else {
            shiftRecord(recordOffset + oldRecordLen, recordLen - oldRecordLen, false);
        }

        // Write Record Data
        memcpy(data + recordOffset, byteSeq, recordLen);

        // Update Record Length in slot
        setRecordLen(slotIndex, recordLen);

        // Flush page
        ret = fh.writePage(pageNum, data);
        if(ret) {
            LOG(ERROR) << "Fail to flush page data into file! @ RecordPageHandle::updateRecord" << std::endl;
            return ret;
        }
        return 0;
    }

    // Record Pointer Format
    // | Mask | Page Index | SlotIndex|
    // | 1 |   4   |  2  |
    RC RecordPageHandle::setRecordPointToNewRecord(int16_t curSlotIndex, const RID& newRecordPos) {
        int16_t recordOffset = getRecordOffset(curSlotIndex);
        int16_t oldRecordLen = getRecordLen(curSlotIndex);
        // Set record mask
        setRecordMask(curSlotIndex, RECORD_MASK_PTRRECORD);
        // Write page index
        memcpy(data + recordOffset + RECORD_MASK_LEN, &newRecordPos.pageNum, PTRRECORD_PAGE_INDEX_LEN);
        // Write slot index
        memcpy(data + recordOffset + RECORD_MASK_LEN + PTRRECORD_PAGE_INDEX_LEN, &newRecordPos.slotNum, PTRRECORD_SLOT_INDEX_LEN);

        // Update Record Length
        int16_t newRecordLen = RECORD_MASK_LEN + PTRRECORD_PAGE_INDEX_LEN + PTRRECORD_SLOT_INDEX_LEN;
        setRecordLen(curSlotIndex, newRecordLen);

        // Shift records left
        int16_t dist = oldRecordLen - newRecordLen;
        shiftRecord(recordOffset + oldRecordLen, dist, true);

        return 0;
    }

    /*
     * Helper Functions
     */

    // Slots whose index > slotNeedUpdate will be updated
    // Free byte pointer will be updated
    RC RecordPageHandle::shiftRecord(int16_t dataNeedShiftStartPos, int16_t dist, bool shiftLeft){
        int16_t dataNeedMoveLen = freeBytePointer - dataNeedShiftStartPos;
        if(dataNeedMoveLen < 0) {
            return ERR_IMPOSSIBLE;
        }

        if(dataNeedMoveLen != 0) {
            // Must Use Memmove! Source and Destination May Overlap
            if(shiftLeft)
                memmove(data + dataNeedShiftStartPos - dist, data + dataNeedShiftStartPos, dataNeedMoveLen);
            else
                memmove(data + dataNeedShiftStartPos + dist, data + dataNeedShiftStartPos, dataNeedMoveLen);

            // Update the slots of record that follows dataNeedShift
            int16_t curRecordOffset;
            for (int16_t i = 1; i <= slotCounter; i++) {
                curRecordOffset = getRecordOffset(i);
                if(curRecordOffset == PAGE_EMPTY_SLOT_OFFSET || curRecordOffset < dataNeedShiftStartPos) {
                    continue;
                }
                if(shiftLeft)
                    curRecordOffset -= dist;
                else
                    curRecordOffset += dist;
                memcpy(data + getSlotOffset(i), &curRecordOffset, PAGE_SLOT_RECORD_PTR_LEN);
            }
        }

        // Update free byte pointer
        if(shiftLeft)
            freeBytePointer -= dist;
        else
            freeBytePointer += dist;
        setFreeBytePointer(freeBytePointer);

        return 0;
    }

    int16_t RecordPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePointer - getSlotListLen() - PAGE_HEADER_LEN;
    }

    bool RecordPageHandle::hasEnoughSpaceForRecord(int16_t recordLen) {
        int16_t freeSpace = getFreeSpace();
        return freeSpace >= (recordLen + PAGE_SLOT_LEN);
    }

    int16_t RecordPageHandle::findAvailSlot() {
        // Traverse through all existing slots searching for an empty slot
        int16_t index = 0;
        for(int16_t i = 1; i <= slotCounter; i++) {
            if(isRecordDeleted(i)) {
                index = i;
                break;
            }
        }

        if(index > 0) {     // Find an empty slot
            return index;
        }
        else {      // Allocate a new slot
            slotCounter++;
            setSlotCounter(slotCounter);
            return slotCounter;
        }
    }

    int16_t RecordPageHandle::getSlotListLen() {
        return PAGE_SLOT_LEN * slotCounter;
    }

    int16_t RecordPageHandle::getFreeBytePointerOffset() {
        return PAGE_SIZE - PAGE_FREEBYTE_PTR_LEN;
    }
    int16_t RecordPageHandle::getFreeBytePointer() {
        int16_t ptr;
        memcpy(&ptr, data + getFreeBytePointerOffset(), PAGE_FREEBYTE_PTR_LEN);
        return ptr;
    }
    void RecordPageHandle::setFreeBytePointer(int16_t ptr) {
        memcpy(data + getFreeBytePointerOffset(), &ptr, PAGE_FREEBYTE_PTR_LEN);
    }

    int16_t RecordPageHandle::getSlotCounterOffset() {
        return PAGE_SIZE - PAGE_FREEBYTE_PTR_LEN - PAGE_SLOTCOUNTER_LEN;
    }
    int16_t RecordPageHandle::getSlotCounter() {
        int16_t counter;
        memcpy(&counter, data + getSlotCounterOffset(), PAGE_SLOTCOUNTER_LEN);
        return counter;
    }
    void RecordPageHandle::setSlotCounter(int16_t slotCounter) {
        memcpy(data + getSlotCounterOffset(), &slotCounter, PAGE_SLOTCOUNTER_LEN);
    }

    int16_t RecordPageHandle::getSlotOffset(int16_t slotIndex) {
        return getSlotCounterOffset() - PAGE_SLOT_LEN * slotIndex;
    }

    int16_t RecordPageHandle::getRecordOffset(int16_t slotIndex) {
        int16_t offset;
        memcpy(&offset, data + getSlotOffset(slotIndex), PAGE_SLOT_RECORD_PTR_LEN);
        return offset;
    }
    void RecordPageHandle::setRecordOffset(int16_t slotIndex, int16_t recordOffset) {
        memcpy(data + getSlotOffset(slotIndex), &recordOffset, PAGE_SLOT_RECORD_PTR_LEN);
    }

    int16_t RecordPageHandle::getRecordLen(int16_t slotIndex) {
        int16_t recordLen;
        memcpy(&recordLen, data + getSlotOffset(slotIndex) + PAGE_SLOT_RECORD_PTR_LEN, PAGE_SLOT_RECORD_LEN_LEN);
        return recordLen;
    }
    void RecordPageHandle::setRecordLen(int16_t slotIndex, int16_t recordLen) {
        memcpy(data + getSlotOffset(slotIndex) + PAGE_SLOT_RECORD_PTR_LEN, &recordLen, PAGE_SLOT_RECORD_LEN_LEN);
    }

    int8_t RecordPageHandle::getRecordMask(int16_t slotIndex) {
        int8_t mask;
        memcpy(&mask, data + getRecordOffset(slotIndex), RECORD_MASK_LEN);
        return mask;
    }
    void RecordPageHandle::setRecordMask(int16_t slotIndex, int8_t mask) {
        memcpy(data + getRecordOffset(slotIndex), &mask, RECORD_MASK_LEN);
    }

    int8_t RecordPageHandle::getRecordVersion(int16_t slotIndex) {
        int8_t version;
        memcpy(&version, data + getRecordOffset(slotIndex) + RECORD_MASK_LEN, RECORD_VERSION_LEN);
        return version;
    }
    void RecordPageHandle::setRecordVersion(int16_t slotIndex, int8_t recordVersion) {
        memcpy(data + getRecordOffset(slotIndex) + RECORD_MASK_LEN, &recordVersion, RECORD_VERSION_LEN);
    }

    int16_t RecordPageHandle::getRecordAttrNum(int16_t slotIndex) {
        int16_t attrNum;
        memcpy(&attrNum, data + getRecordOffset(slotIndex) + RECORD_MASK_LEN + RECORD_VERSION_LEN, RECORD_ATTRNUM_LEN);
        return attrNum;
    }
    void RecordPageHandle::setRecordAttrNum(int16_t slotIndex, int16_t attrNum) {
        memcpy(data + getRecordOffset(slotIndex) + RECORD_MASK_LEN + RECORD_VERSION_LEN, &attrNum, RECORD_ATTRNUM_LEN);
    }

    bool RecordPageHandle::isRecordPointer(int16_t slotNum) {
        int8_t mask = getRecordMask(slotNum);
        return mask == RECORD_MASK_PTRRECORD;
    }

    bool RecordPageHandle::isRecordReadable(uint16_t slotIndex) {
        if(slotIndex > slotCounter || isRecordDeleted(slotIndex))
            return false;
        else
            return true;
    }


    int16_t RecordPageHandle::getAttrBeginPos(int16_t slotIndex, int16_t attrIndex) {
        int16_t curOffset = getAttrEndPos(slotIndex, attrIndex);
        if(curOffset == RECORD_ATTR_NULL_ENDPOS) {   // Null attribute
            return ERR_RECORD_NULL;
        }
        int16_t prevOffset = -1;
        // Looking for previous attribute that is not null
        for(int i = attrIndex - 1; i >= 0; i--) {
            prevOffset = getAttrEndPos(slotIndex, i);
            if(prevOffset != RECORD_ATTR_NULL_ENDPOS) {
                break;
            }
        }
        if(prevOffset == -1) {
            prevOffset = RECORD_DICT_BEGIN + getRecordAttrNum(slotIndex) * RECORD_DICT_SLOT_LEN;
        }
        return prevOffset;
    }

    int16_t RecordPageHandle::getAttrEndPos(int16_t slotIndex, int16_t attrIndex) {
        int16_t recordOffset = getRecordOffset(slotIndex);
        int16_t dictOffset = RECORD_DICT_BEGIN + attrIndex * RECORD_DICT_SLOT_LEN;
        int16_t attrEndPos;
        memcpy(&attrEndPos, data + recordOffset + dictOffset, RECORD_DICT_SLOT_LEN);
        return attrEndPos;
    }

    int16_t RecordPageHandle::getAttrLen(int16_t slotIndex, int16_t attrIndex) {
        int16_t curOffset = getAttrEndPos(slotIndex, attrIndex);
        int16_t prevOffset = getAttrBeginPos(slotIndex, attrIndex);
        return curOffset - prevOffset;
    }

    bool RecordPageHandle::isAttrNull(int16_t slotIndex, int16_t attrIndex) {
        int16_t curOffset = getAttrEndPos(slotIndex, attrIndex);
        return curOffset == RECORD_ATTR_NULL_ENDPOS;
    }
}
