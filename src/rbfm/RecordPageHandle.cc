#include "src/include/rbfm.h"

namespace PeterDB {
    RecordPageHandle::RecordPageHandle(FileHandle& fileHandle, PageNum pageNum): fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, data);
        memcpy(&freeBytePointer, data + getFreeBytePointerOffset(), sizeof(short));
        memcpy(&slotCounter, data + getSlotCounterOffset(), sizeof(short));
    }

    RecordPageHandle::~RecordPageHandle() = default;

    short RecordPageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePointer - getSlotListLen() - getHeaderLen();
    }

    bool RecordPageHandle::hasEnoughSpaceForRecord(int recordLen) {
        short freeSpace = getFreeSpace();
        return freeSpace >= (recordLen + 2 * sizeof(short));
    }

    RC RecordPageHandle::insertRecordByteSeq(char byteSeq[], RecordLen recordLen, RID& rid) {
        RC ret = 0;

        // Get Slot Index
        short slotIndex = getAvailSlot();

        // Append a slot(pointer + length) to the end
        memcpy(data + getSlotOffset(slotIndex), &freeBytePointer, sizeof(short));
        memcpy(data + getSlotOffset(slotIndex) + sizeof(short), &recordLen, sizeof(short));

        // Write Byte Seq
        memcpy(data + freeBytePointer, byteSeq, recordLen);
        // Update Free Space Pointer
        freeBytePointer += recordLen;
        memcpy(data + getFreeBytePointerOffset(), &freeBytePointer, sizeof(short));

        // Flush page
        ret = fh.writePage(pageNum, data);

        rid.pageNum = this->pageNum;
        rid.slotNum = slotIndex;

        if(ret) {
            std::cout << "Fail to write new data into file while inserting record! @ RecordPageHandle::insertRecordByteSeq" << std::endl;
            return ret;
        }
        return 0;
    }

    RC RecordPageHandle::getRecordByteSeq(short slotNum, char *recordByteSeq, short& recordLen) {
        RC ret = 0;
        short slotOffset = getSlotOffset(slotNum);
        short recordOffset;

        // Get Record Start Position and Length
        memcpy(&recordOffset, data + slotOffset, sizeof(short));
        memcpy(&recordLen, data + slotOffset + sizeof(short), sizeof(short));

        // Read Record Byte Seq
        memcpy(recordByteSeq, data + recordOffset, recordLen);

        return 0;
    }

    short RecordPageHandle::getAvailSlot() {
        // Traverse through all existing slots searching for an empty slot
        short curSlotPtr;
        short index = 0;
        for(short i = 1; i <= slotCounter; i++) {
            memcpy(&curSlotPtr, data + getSlotOffset(i), sizeof(short));
            if(curSlotPtr < 0) {
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

    short RecordPageHandle::getSlotOffset(short slotNum) {
        return getSlotCounterOffset() - 2 * sizeof(short) * slotNum;
    }
}
