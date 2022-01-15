#include "src/include/rbfm.h"

namespace PeterDB {
    PageHandle::PageHandle(FileHandle& fileHandle, PageNum pageNum):fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, data);
        memcpy(&freeBytePointer, data + getFreeBytePointerOffset(), sizeof(short));
        memcpy(&slotCounter, data + getSlotCounterOffset(), sizeof(short));
    }

    PageHandle::~PageHandle() = default;

    short PageHandle::getFreeSpace() {
        return PAGE_SIZE - freeBytePointer - getSlotListLen() - getHeaderLen();
    }

    bool PageHandle::hasEnoughSpaceForRecord(int recordLen) {
        short freeSpace = getFreeSpace();
        return freeSpace >= (recordLen + 2 * sizeof(short));
    }

    RC PageHandle::insertRecordByteSeq(char byteSeq[], RecordLen recordLen, RID& rid) {
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
            std::cout << "Fail to write new data into file while inserting record! @ PageHandle::insertRecordByteSeq" << std::endl;
            return ret;
        }
        return 0;
    }

    RC PageHandle::getRecordByteSeq(short slotNum, char *recordByteSeq, short& recordLen) {
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

    short PageHandle::getHeaderLen() {
        return 2 * sizeof(short);
    }

    short PageHandle::getSlotListLen() {
        return 2 * sizeof(short) * slotCounter;
    }

    short PageHandle::getSlotCounterOffset() {
        return PAGE_SIZE - 2 * sizeof(short);
    }

    short PageHandle::getFreeBytePointerOffset() {
        return PAGE_SIZE - sizeof(short);
    }

    short PageHandle::getSlotOffset(short slotNum) {
        return getSlotCounterOffset() - 2 * sizeof(short) * slotNum;
    }
}
