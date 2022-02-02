#include "src/include/rbfm.h"

namespace PeterDB {
    RecordPageHandle::RecordPageHandle(FileHandle& fileHandle, PageNum pageNum): fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, data);
        memcpy(&freeBytePointer, data + getFreeBytePointerOffset(), sizeof(short));
        memcpy(&slotCounter, data + getSlotCounterOffset(), sizeof(short));
    }

    RecordPageHandle::~RecordPageHandle() {
        fh.writePage(pageNum, data);
    }

    /*
     * Read Record
     */

    RC RecordPageHandle::getRecordByteSeq(short slotNum, char *recordByteSeq, short& recordLen) {
        if(slotNum > slotCounter || isRecordDeleted(slotNum)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << slotNum << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::getRecordByteSeq" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }

        short recordOffset = getRecordOffset(slotNum);
        recordLen = getRecordLen(slotNum);

        // Read Record Byte Seq
        memcpy(recordByteSeq, data + recordOffset, recordLen);

        return 0;
    }

    RC RecordPageHandle::getRecordPointerTarget(const short curSlotNum, int& ptrPageNum, short& ptrSlotNum) {
        if(curSlotNum > slotCounter || isRecordDeleted(curSlotNum)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << curSlotNum << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::getRecordPointerTarget" << std::endl;
            return 1;
        }

        short recordOffset = getRecordOffset(curSlotNum);
        short mask;
        memcpy(&mask, data + recordOffset, sizeof(short));
        if(mask != 0x1) {
            LOG(ERROR) << "Record is not a pointer!" << std::endl;
            return -1;
        }
        memcpy(&ptrPageNum, data + recordOffset + sizeof(short), sizeof(int));
        memcpy(&ptrSlotNum, data + recordOffset + sizeof(short) + sizeof(int), sizeof(short));
        return 0;
    }

    RC RecordPageHandle::getRecordAttr(short slotNum, short attrIndex, uint8_t* attrData) {
        if(slotNum > slotCounter || isRecordDeleted(slotNum)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << slotNum << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::getRecordAttr" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }

        short recordOffset = getRecordOffset(slotNum);
        short attrOffset = getAttrOffset(slotNum, attrIndex);
        short attrLen = getAttrLen(slotNum, attrIndex);
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
        ret = getRecordByteSeq(slotIndex, (char *)byteSeq, (short& )recordLen);
        if(ret) {
            return ret;
        }
        return 0;
    }

    /*
     * Insert Record
     */
    RC RecordPageHandle::insertRecord(char byteSeq[], short byteSeqLen, RID& rid) {
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
        memcpy(data + getFreeBytePointerOffset(), &freeBytePointer, sizeof(short));

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
    RC RecordPageHandle::deleteRecord(const int slotIndex) {
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
        short recordOffset = getRecordOffset(slotIndex);
        short recordLen = getRecordLen(slotIndex);

        // Shift records left to fill the emtpy hole
        ret = shiftRecord(recordOffset + recordLen, recordLen, true);
        if(ret) {
            LOG(ERROR) << "Fail to shift records left @ RecordPageHandle::deleteRecord" << std::endl;
            return ret;
        }

        // Update current slot to make it empty
        recordOffset = EMPTY_SLOT_OFFSET;
        recordLen = EMPTY_SLOT_LEN;
        memcpy(data + getSlotOffset(slotIndex), &recordOffset, sizeof(short));
        memcpy(data + getSlotOffset(slotIndex) + sizeof(short), &recordLen, sizeof(short));

        // Flush page to disk, actually OS handle it, may not flush to disk immediately
        ret = fh.writePage(pageNum, data);
        if(ret) {
            LOG(ERROR) << "Fail to flush page data into file @ RecordPageHandle::deleteRecord" << std::endl;
        }
        return ret;
    }

    bool RecordPageHandle::isRecordDeleted(short slotIndex) {
        short recordOffset = getRecordOffset(slotIndex);
        return recordOffset < 0;
    }

    /*
     * Update Record
     */
    RC RecordPageHandle::updateRecord(short slotIndex, char byteSeq[], short recordLen) {
        if(slotIndex > slotCounter || isRecordDeleted(slotIndex)) {
            LOG(ERROR) << "Slot not exist or deleted! SlotIndex: " << slotIndex << ", SlotCounter: " << slotCounter << " @ RecordPageHandle::deleteRecord" << std::endl;
            return ERR_SLOT_NOT_EXIST_OR_DELETED;
        }

        short recordOffset = getRecordOffset(slotIndex);
        short oldRecordLen = getRecordLen(slotIndex);

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
        memcpy(data + getSlotOffset(slotIndex) + sizeof(short), &recordLen, sizeof(short));

        return 0;
    }

    // Record Pointer Format
    // | Mask | Page Index | SlotIndex|
    // | 2 |   4   | 2 |
    RC RecordPageHandle::setRecordPointToNewRecord(short curSlotIndex, const RID& newRecordPos) {
        short recordOffset = getRecordOffset(curSlotIndex);
        short oldRecordLen = getRecordLen(curSlotIndex);

        // Write Mask
        short mask = MASK_RECORD_POINTER;
        memcpy(data + recordOffset, &mask, sizeof(short));
        // Write page index
        memcpy(data + recordOffset + sizeof(short), &newRecordPos.pageNum, sizeof(unsigned));
        // Write slot index
        memcpy(data + recordOffset + sizeof(short) + sizeof(unsigned), &newRecordPos.slotNum, sizeof(short));

        // Update Record Length
        short newRecordLen = sizeof(short) + sizeof(unsigned) + sizeof(short);
        memcpy(data + getSlotOffset(newRecordPos.slotNum) + sizeof(short), &newRecordLen, sizeof(short));
        // Shift records left
        short dist = oldRecordLen - newRecordLen;
        shiftRecord(recordOffset + oldRecordLen, dist, true);

        return 0;
    }

    /*
     * Helper Functions
     */

    // Slots whose index > slotNeedUpdate will be updated
    // Free byte pointer will be updated
    RC RecordPageHandle::shiftRecord(short dataNeedShiftStartPos, short dist, bool shiftLeft){
        short dataNeedMoveLen = freeBytePointer - dataNeedShiftStartPos;

        if(dataNeedMoveLen != 0) {
            // Must Use Memmove! Source and Destination May Overlap
            if(shiftLeft)
                memmove(data + dataNeedShiftStartPos - dist, data + dataNeedShiftStartPos, dataNeedMoveLen);
            else
                memmove(data + dataNeedShiftStartPos + dist, data + dataNeedShiftStartPos, dataNeedMoveLen);

            // Update the slots of record that follows dataNeedShift
            short curRecordOffset;
            for (int i = 1; i <= slotCounter; i++) {
                memcpy(&curRecordOffset, data + getSlotOffset(i), sizeof(short));
                if(curRecordOffset == ERR_SLOT_EMPTY || curRecordOffset < dataNeedShiftStartPos) {
                    continue;
                }
                if(shiftLeft)
                    curRecordOffset -= dist;
                else
                    curRecordOffset += dist;
                memcpy(data + getSlotOffset(i), &curRecordOffset, sizeof(short));
            }
        }

        // Update free byte pointer
        if(shiftLeft)
            freeBytePointer -= dist;
        else
            freeBytePointer += dist;
        memcpy(data + getFreeBytePointerOffset(), &freeBytePointer, sizeof(short));

        return 0;
    }

    short RecordPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePointer - getSlotListLen() - getHeaderLen();
    }

    bool RecordPageHandle::hasEnoughSpaceForRecord(int recordLen) {
        short freeSpace = getFreeSpace();
        return freeSpace >= (recordLen + 2 * sizeof(short));
    }

    short RecordPageHandle::getAvailSlot() {
        // Traverse through all existing slots searching for an empty slot
        short index = 0;
        for(short i = 1; i <= slotCounter; i++) {
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
            memcpy(data + getSlotCounterOffset(), &slotCounter, sizeof(short));
            return slotCounter;
        }
    }

    short RecordPageHandle::getHeaderLen() {
        return 2 * sizeof(short);
    }

    short RecordPageHandle::getSlotListLen() {
        return 2 * sizeof(short) * slotCounter;
    }

    short RecordPageHandle::getSlotCounterOffset() {
        return PAGE_SIZE - 2 * sizeof(short);
    }

    short RecordPageHandle::getFreeBytePointerOffset() {
        return PAGE_SIZE - sizeof(short);
    }

    short RecordPageHandle::getSlotOffset(short slotIndex) {
        return getSlotCounterOffset() - 2 * sizeof(short) * slotIndex;
    }

    short RecordPageHandle::getRecordOffset(short slotIndex) {
        short offset;
        memcpy(&offset, data + getSlotOffset(slotIndex), sizeof(short));
        return offset;
    }

    short RecordPageHandle::getRecordLen(short slotIndex) {
        short recordLen;
        memcpy(&recordLen, data + getSlotOffset(slotIndex) + sizeof(short), sizeof(short));
        return recordLen;
    }

    bool RecordPageHandle::isRecordPointer(short slotNum) {
        short recordOffset = getRecordOffset(slotNum);
        short mask;
        memcpy(&mask, data + recordOffset, sizeof(short));
        return mask == 0x1;
    }

    bool RecordPageHandle::isRecordReadable(uint16_t slotIndex) {
        if(slotIndex > slotCounter || isRecordDeleted(slotIndex))
            return false;
        else
            return true;
    }

    short RecordPageHandle::getAttrNum(short slotIndex) {
        short recordOffset = getRecordOffset(slotIndex);
        short attrNum;
        memcpy(&attrNum, data + recordOffset + sizeof(short), sizeof(short));
        return attrNum;
    }

    short RecordPageHandle::getAttrOffset(short slotIndex, short attrIndex) {
        short recordOffset = getRecordOffset(slotIndex);
        short dictOffset = sizeof(short) * 2 + attrIndex * sizeof(short);
        short attrOffset;
        memcpy(&attrOffset, data + recordOffset + dictOffset, sizeof(short));
        return attrOffset;
    }

    short RecordPageHandle::getAttrLen(short slotIndex, short attrIndex) {
        short curOffset = getAttrOffset(slotIndex, attrIndex);
        if(curOffset == -1) {   // Null attribute
            return -1;
        }
        short prevOffset = -1;
        // Looking for previous attribute that is not null
        for(int i = slotIndex - 1; i >= 0; i--) {
            prevOffset = getAttrOffset(slotIndex, i);
            if(prevOffset != -1) {
                break;
            }
        }
        if(prevOffset == -1) {
            prevOffset = (2 + getAttrNum(slotIndex)) * sizeof(short);
        }
        return curOffset - prevOffset;
    }
}
