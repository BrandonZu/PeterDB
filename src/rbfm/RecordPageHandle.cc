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

        // Update Total Slot Counter
        slotCounter++;
        memcpy(data + getSlotCounterOffset(), &slotCounter, sizeof(short));
        // Add a Slot to the end
        memcpy(data + getSlotOffset(slotCounter), &freeBytePointer, sizeof(short));
        memcpy(data + getSlotOffset(slotCounter) + sizeof(short), &recordLen, sizeof(short));

        // Write Byte Seq
        memcpy(data + freeBytePointer, byteSeq, recordLen);
        // Update Free Space Pointer
        freeBytePointer += recordLen;
        memcpy(data + getFreeBytePointerOffset(), &freeBytePointer, sizeof(short));

        // Flush page
        ret = fh.writePage(pageNum, data);

        rid.pageNum = this->pageNum;
        rid.slotNum = this->slotCounter;

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
