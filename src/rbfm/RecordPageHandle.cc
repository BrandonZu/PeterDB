#include "src/include/rbfm.h"

namespace PeterDB {
    RecordPageHandle::RecordPageHandle(FileHandle& fileHandle, PageNum pageNum): fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, data);
        memcpy(&freeBytePointer, data + getFreeBytePointerOffset(), sizeof(int16_t));
        memcpy(&slotCounter, data + getSlotCounterOffset(), sizeof(int16_t));
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
            return 1;
        }

        int16_t recordOffset = getRecordOffset(curSlotNum);
        int16_t mask;
        memcpy(&mask, data + recordOffset, sizeof(int16_t));
        if(mask != 0x1) {
            LOG(ERROR) << "Record is not a pointer!" << std::endl;
            return -1;
        }
        memcpy(&ptrPageNum, data + recordOffset + sizeof(int16_t), sizeof(int));
        memcpy(&ptrSlotNum, data + recordOffset + sizeof(int16_t) + sizeof(int), sizeof(int16_t));
        return 0;
    }

    RC RecordPageHandle::getRecordAttr(int16_t slotNum, int16_t attrIndex, uint8_t* attrData) {
        if(slotNum > slotCounter || isRecordDeleted(slotNum)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << slotNum << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::getRecordAttr" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }

        int16_t recordOffset = getRecordOffset(slotNum);
        int16_t attrOffset = getAttrOffset(slotNum, attrIndex);
        int16_t attrLen = getAttrLen(slotNum, attrIndex);
        memcpy(attrData, data + recordOffset + attrOffset, attrLen);

        return 0;
    }

    RC RecordPageHandle::getNextRecord(uint16_t& slotIndex, uint8_t* byteSeq, uint16_t& recordLen) {
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
        ret = getRecordByteSeq(slotIndex, (uint8_t *)byteSeq, (int16_t& )recordLen);
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
        int16_t slotIndex = getAvailSlot();

        // Pad the data if necessary and append padded record data to the end
        uint16_t recordLen = std::max(byteSeqLen, MIN_RECORD_LEN);
        memcpy(data + freeBytePointer, byteSeq, byteSeqLen);

        // Fill in offset and length of record
        memcpy(data + getSlotOffset(slotIndex), &freeBytePointer, sizeof(int16_t));
        memcpy(data + getSlotOffset(slotIndex) + sizeof(int16_t), &recordLen, sizeof(int16_t));

        // Update Free Space Pointer
        freeBytePointer += recordLen;
        memcpy(data + getFreeBytePointerOffset(), &freeBytePointer, sizeof(int16_t));

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
        recordOffset = EMPTY_SLOT_OFFSET;
        recordLen = EMPTY_SLOT_LEN;
        memcpy(data + getSlotOffset(slotIndex), &recordOffset, sizeof(int16_t));
        memcpy(data + getSlotOffset(slotIndex) + sizeof(int16_t), &recordLen, sizeof(int16_t));

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
        memcpy(data + getSlotOffset(slotIndex) + sizeof(int16_t), &recordLen, sizeof(int16_t));

        return 0;
    }

    // Record Pointer Format
    // | Mask | Page Index | SlotIndex|
    // | 2 |   4   | 2 |
    RC RecordPageHandle::setRecordPointToNewRecord(int16_t curSlotIndex, const RID& newRecordPos) {
        int16_t recordOffset = getRecordOffset(curSlotIndex);
        int16_t oldRecordLen = getRecordLen(curSlotIndex);

        // Write Mask
        int16_t mask = MASK_RECORD_POINTER;
        memcpy(data + recordOffset, &mask, sizeof(int16_t));
        // Write page index
        memcpy(data + recordOffset + sizeof(int16_t), &newRecordPos.pageNum, sizeof(unsigned));
        // Write slot index
        memcpy(data + recordOffset + sizeof(int16_t) + sizeof(unsigned), &newRecordPos.slotNum, sizeof(int16_t));

        // Update Record Length
        int16_t newRecordLen = sizeof(int16_t) + sizeof(unsigned) + sizeof(int16_t);
        memcpy(data + getSlotOffset(newRecordPos.slotNum) + sizeof(int16_t), &newRecordLen, sizeof(int16_t));
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

        if(dataNeedMoveLen != 0) {
            // Must Use Memmove! Source and Destination May Overlap
            if(shiftLeft)
                memmove(data + dataNeedShiftStartPos - dist, data + dataNeedShiftStartPos, dataNeedMoveLen);
            else
                memmove(data + dataNeedShiftStartPos + dist, data + dataNeedShiftStartPos, dataNeedMoveLen);

            // Update the slots of record that follows dataNeedShift
            int16_t curRecordOffset;
            for (int i = 1; i <= slotCounter; i++) {
                memcpy(&curRecordOffset, data + getSlotOffset(i), sizeof(int16_t));
                if(curRecordOffset == ERR_SLOT_EMPTY || curRecordOffset < dataNeedShiftStartPos) {
                    continue;
                }
                if(shiftLeft)
                    curRecordOffset -= dist;
                else
                    curRecordOffset += dist;
                memcpy(data + getSlotOffset(i), &curRecordOffset, sizeof(int16_t));
            }
        }

        // Update free byte pointer
        if(shiftLeft)
            freeBytePointer -= dist;
        else
            freeBytePointer += dist;
        memcpy(data + getFreeBytePointerOffset(), &freeBytePointer, sizeof(int16_t));

        return 0;
    }

    int16_t RecordPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePointer - getSlotListLen() - getHeaderLen();
    }

    bool RecordPageHandle::hasEnoughSpaceForRecord(int16_t recordLen) {
        int16_t freeSpace = getFreeSpace();
        return freeSpace >= (recordLen + 2 * sizeof(int16_t));
    }

    int16_t RecordPageHandle::getAvailSlot() {
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
            memcpy(data + getSlotCounterOffset(), &slotCounter, sizeof(int16_t));
            return slotCounter;
        }
    }

    int16_t RecordPageHandle::getHeaderLen() {
        return 2 * sizeof(int16_t);
    }

    int16_t RecordPageHandle::getSlotListLen() {
        return 2 * sizeof(int16_t) * slotCounter;
    }

    int16_t RecordPageHandle::getSlotCounterOffset() {
        return PAGE_SIZE - 2 * sizeof(int16_t);
    }

    int16_t RecordPageHandle::getFreeBytePointerOffset() {
        return PAGE_SIZE - sizeof(int16_t);
    }

    int16_t RecordPageHandle::getSlotOffset(int16_t slotIndex) {
        return getSlotCounterOffset() - 2 * sizeof(int16_t) * slotIndex;
    }

    int16_t RecordPageHandle::getRecordOffset(int16_t slotIndex) {
        int16_t offset;
        memcpy(&offset, data + getSlotOffset(slotIndex), sizeof(int16_t));
        return offset;
    }

    int16_t RecordPageHandle::getRecordLen(int16_t slotIndex) {
        int16_t recordLen;
        memcpy(&recordLen, data + getSlotOffset(slotIndex) + sizeof(int16_t), sizeof(int16_t));
        return recordLen;
    }

    bool RecordPageHandle::isRecordPointer(int16_t slotNum) {
        int16_t recordOffset = getRecordOffset(slotNum);
        int16_t mask;
        memcpy(&mask, data + recordOffset, sizeof(int16_t));
        return mask == 0x1;
    }

    bool RecordPageHandle::isRecordReadable(uint16_t slotIndex) {
        if(slotIndex > slotCounter || isRecordDeleted(slotIndex))
            return false;
        else
            return true;
    }

    int16_t RecordPageHandle::getAttrNum(int16_t slotIndex) {
        int16_t recordOffset = getRecordOffset(slotIndex);
        int16_t attrNum;
        memcpy(&attrNum, data + recordOffset + sizeof(int16_t), sizeof(int16_t));
        return attrNum;
    }

    int16_t RecordPageHandle::getAttrOffset(int16_t slotIndex, int16_t attrIndex) {
        int16_t recordOffset = getRecordOffset(slotIndex);
        int16_t dictOffset = sizeof(int16_t) * 2 + attrIndex * sizeof(int16_t);
        int16_t attrOffset;
        memcpy(&attrOffset, data + recordOffset + dictOffset, sizeof(int16_t));
        return attrOffset;
    }

    int16_t RecordPageHandle::getAttrLen(int16_t slotIndex, int16_t attrIndex) {
        int16_t curOffset = getAttrOffset(slotIndex, attrIndex);
        if(curOffset == -1) {   // Null attribute
            return -1;
        }
        int16_t prevOffset = -1;
        // Looking for previous attribute that is not null
        for(int i = slotIndex - 1; i >= 0; i--) {
            prevOffset = getAttrOffset(slotIndex, i);
            if(prevOffset != -1) {
                break;
            }
        }
        if(prevOffset == -1) {
            prevOffset = (2 + getAttrNum(slotIndex)) * sizeof(int16_t);
        }
        return curOffset - prevOffset;
    }
}
